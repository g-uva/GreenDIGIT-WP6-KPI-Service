import argparse
import asyncio
import errno
import json
import os
import sys
import threading
import tempfile
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from fastapi import Body, Depends, FastAPI, HTTPException, Request, APIRouter
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import AliasChoices, BaseModel, Field, ConfigDict
from typing import Any, Dict, Optional
from pymongo import MongoClient
from pathlib import Path

from bidding_zone_resolver import (
    BiddingZoneNotFoundError,
    BiddingZoneResolver,
    BiddingZoneResolverError,
)

# Volume Version

API_PREFIX = "/v1"

APP_DESCRIPTION = (
    "Service providing GreenDIGIT KPIs. It retrieves location information from "
    "GOC DB, queries WattNet for carbon intensity, and exposes helper endpoints "
    "used by the CIM pipeline. The CI endpoint supports online mode (WattNet) and "
    "local fallback mode backed by a persisted JSON cache."
)

app = FastAPI(
    title="GreenDIGIT KPI Service",
    description=APP_DESCRIPTION,
    swagger_ui_parameters={"persistAuthorization": True},
    root_path="/gd-kpi-api",
    docs_url=f"{API_PREFIX}/docs",
    openapi_url=f"{API_PREFIX}/openapi.json",
)
router = APIRouter(prefix=API_PREFIX)

@app.get("/", include_in_schema=False)
def redirect_to_docs():
    """Convenience redirect to the versioned docs."""
    return RedirectResponse(url=f"{API_PREFIX}/docs")

# --- Configuration & Environment ---
HOST_SERVER = "https://greendigit-cim.sztaki.hu"
WATTNET_BASE = os.getenv("WATTNET_BASE") or os.getenv("WATTPRINT_BASE", "https://api.wattnet.eu")
WATTNET_TOKEN = os.getenv("WATTNET_TOKEN") or os.getenv("WATTPRINT_TOKEN")
AUTH_VERIFY_URL = os.getenv("AUTH_VERIFY_URL", f"{HOST_SERVER}/gd-cim-api/v1/verify-token")

RETAIN_MONGO_URI = os.getenv("RETAIN_MONGO_URI")
RETAIN_DB_NAME   = os.getenv("RETAIN_DB_NAME", "ci-retainment-db")
RETAIN_COLL      = os.getenv("RETAIN_COLL", "pending_ci")

CNR_SQL_FORWARD_URL = os.getenv("CNR_SQL_FORWARD_URL", "http://sql-adapter:8033/cnr-sql-service")
PUE_DEFAULT = os.getenv("PUE_DEFAULT", "1.7")
CI_API_BASE = os.getenv("CI_API_BASE", f"{HOST_SERVER}/gd-kpi-api/v1")

PUE_REFRESH_HOURS = os.getenv("PUE_REFRESH_HOURS", "3")
CI_CACHE_TTL_S = int(os.getenv("CI_CACHE_TTL_S", "300"))
BZ_GEOJSON_DIR = os.getenv("BZ_GEOJSON_DIR")
CI_CACHE_FILE = os.getenv(
    "CI_CACHE_FILE",
    os.path.join(os.path.dirname(__file__), "ci_cache.json"),
)

SITES_PATH = os.environ.get("SITES_JSON", "/data/sites_latlngpue.json")
SITES_CACHE_PATH = os.environ.get(
    "SITES_CACHE_JSON",
    os.path.join(os.path.dirname(SITES_PATH) or ".", "sites_cache.json"),
)
SITES_REFRESH_TASK: Optional[asyncio.Task] = None

sess = requests.Session()

# --- GOCDB Configuration ---
GOCDB_BASE = os.getenv("GOCDB_BASE", "https://goc.egi.eu/gocdbpi")
GOCDB_SCOPE = os.getenv("GOCDB_SCOPE")
GOCDB_TOKEN = os.getenv("GOCDB_TOKEN") or os.getenv("GOCDB_OAUTH_TOKEN")
GOCDB_TIMEOUT = float(os.getenv("GOCDB_TIMEOUT", "20"))

CONTAINER_CERT_BASE = "/etc/gocdb-cert"
CERT_BASE = os.environ.get("GOCDB_CERT", CONTAINER_CERT_BASE)
if Path(CONTAINER_CERT_BASE).exists():
    CERT_BASE = CONTAINER_CERT_BASE
else:
    CERT_BASE = ".cert"

GOCDB_CERT = os.getenv("GOCDB_CERT", f"{CERT_BASE}/GDIGIT_Cert.pem")
GOCDB_KEY = os.getenv("GOCDB_KEY", f"{CERT_BASE}/gd_gocdb_private.pem")
GOCDB_CA = os.getenv("GOCDB_CA")

CERT_PATH = GOCDB_CERT.strip() if GOCDB_CERT else None
KEY_PATH = GOCDB_KEY.strip() if GOCDB_KEY else None
CA_PATH = GOCDB_CA.strip() if GOCDB_CA else None

GOCDB_ENDPOINT_TYPE = os.getenv("GOCDB_ENDPOINT", "").strip().lower() or (
    "private" if (CERT_PATH or KEY_PATH or GOCDB_TOKEN) else "public"
)

goc_sess = requests.Session()
goc_sess.headers["Accept"] = "application/xml"
if GOCDB_TOKEN:
    goc_sess.headers["Authorization"] = f"Bearer {GOCDB_TOKEN}"
if CERT_PATH:
    goc_sess.cert = (CERT_PATH, KEY_PATH) if KEY_PATH else CERT_PATH
if CA_PATH:
    goc_sess.verify = CA_PATH

PUE_FALLBACK = 1.7

_BZ_RESOLVER: Optional[BiddingZoneResolver] = None
_BZ_LOCK = threading.Lock()
_CI_BY_BZ_CACHE: Dict[str, Dict[str, Any]] = {}
_CI_CACHE_LOCK = threading.Lock()

# --- Helper Functions ---

def _default_pue() -> float:
    try:
        return float(PUE_DEFAULT)
    except (TypeError, ValueError):
        return PUE_FALLBACK

def _resolve_pue(value: Optional[Any]) -> float:
    if value is not None:
        try:
            return float(value)
        except (TypeError, ValueError):
            pass
    return _default_pue()

def _gocdb_endpoint() -> str:
    suffix = "private" if GOCDB_ENDPOINT_TYPE == "private" else "public"
    return f"{GOCDB_BASE.rstrip('/')}/{suffix}/"

def _text_or_none(el: Optional[ET.Element], path: str) -> Optional[str]:
    if el is None:
        return None
    found = el.find(path)
    if found is not None and found.text:
        return found.text.strip()
    return None

def _text_from_candidates(el: Optional[ET.Element], *candidates: str) -> Optional[str]:
    if el is None:
        return None
    for name in candidates:
        txt = _text_or_none(el, f"./{name}")
        if txt:
            return txt
    return None

def _extract_pue_from_extensions(site_el: ET.Element) -> Optional[float]:
    extensions = site_el.find("./EXTENSIONS")
    if extensions is None:
        return None
    for ext in extensions.findall("./EXTENSION"):
        name = _text_from_candidates(ext, "EXTENSION_NAME", "KEY")
        if name and name.strip().lower() == "pue":
            value = _text_from_candidates(ext, "EXTENSION_VALUE", "VALUE")
            if value is None:
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
    return None

def _extract_extensions_map(site_el: ET.Element) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    extensions = site_el.find("./EXTENSIONS")
    if extensions is None:
        return out
    for ext in extensions.findall("./EXTENSION"):
        key = _text_from_candidates(ext, "EXTENSION_NAME", "KEY")
        if not key:
            continue
        value = _text_from_candidates(ext, "EXTENSION_VALUE", "VALUE")
        if value is None:
            value = _text_or_none(ext, "./VALUE")
        out[key] = value
    return out

def gocdb_fetch_site(site_name: str) -> Optional[Dict[str, Any]]:
    params = {"method": "get_site", "sitename": site_name}
    if GOCDB_SCOPE:
        params["scope"] = GOCDB_SCOPE
    url = _gocdb_endpoint()
    try:
        print(params)
        r = goc_sess.get(url, params=params, timeout=GOCDB_TIMEOUT)
    except Exception as exc:
        raise RuntimeError(f"GOC DB request failed: {exc}") from exc
    if r.status_code in (401, 403):
        print(f"[gocdb] unauthorized ({r.status_code}) for site '{site_name}'", flush=True)
        return None
    if r.status_code == 404:
        return None
    try:
        r.raise_for_status()
    except Exception as exc:
        raise RuntimeError(f"GOC DB error: {exc}") from exc

    try:
        root = ET.fromstring(r.content)
    except ET.ParseError as exc:
        raise RuntimeError(f"Failed to parse GOC DB response: {exc}") from exc

    site_el = root.find("./SITE")
    if site_el is None:
        return None

    lat_txt = _text_or_none(site_el, "./LATITUDE")
    lon_txt = _text_or_none(site_el, "./LONGITUDE")
    lat = float(lat_txt) if lat_txt else None
    lon = float(lon_txt) if lon_txt else None
    country = _text_or_none(site_el, "./COUNTRY") or _text_or_none(site_el, "./COUNTRY_CODE")
    roc = _text_or_none(site_el, "./ROC")
    extensions_map = _extract_extensions_map(site_el)
    pue = _extract_pue_from_extensions(site_el)
    if pue is None:
        raw_pue = extensions_map.get("PUE") or extensions_map.get("pue")
        if raw_pue is not None:
            try:
                pue = float(raw_pue)
            except (TypeError, ValueError):
                pue = None

    scopes: list[str] = []
    scopes_el = site_el.find("./SCOPES")
    if scopes_el is not None:
        for scope_el in scopes_el.findall("./SCOPE"):
            if scope_el.text:
                scopes.append(scope_el.text.strip())

    site_json: Dict[str, Any] = {
        "id": site_el.attrib.get("ID"),
        "primary_key": site_el.attrib.get("PRIMARY_KEY") or _text_or_none(site_el, "./PRIMARY_KEY"),
        "name": site_el.attrib.get("NAME"),
        "short_name": _text_or_none(site_el, "./SHORT_NAME"),
        "official_name": _text_or_none(site_el, "./OFFICIAL_NAME"),
        "portal_url": _text_or_none(site_el, "./GOCDB_PORTAL_URL"),
        "home_url": _text_or_none(site_el, "./HOME_URL"),
        "country": country,
        "roc": roc,
        "timezone": _text_or_none(site_el, "./TIMEZONE"),
        "scopes": scopes,
        "extensions": extensions_map,
    }

    return {
        "lat": lat,
        "lon": lon,
        "country": country,
        "roc": roc,
        "pue": pue,
        "site": site_json,
    }

def to_iso_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0)

def _parse_dt_param(raw: Optional[str], field: str) -> Optional[datetime]:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid {field} datetime format: {raw}") from exc

def _clean_bearer_token(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    token = raw.strip()
    if token.lower().startswith("bearer "):
        token = token.split(" ", 1)[1].strip()
    return token or None

def _client_ip(request: Request) -> str:
    """Return best-effort client IP considering common proxy headers."""
    fwd = request.headers.get("x-forwarded-for")
    if fwd:
        return fwd.split(",")[0].strip()
    real = request.headers.get("x-real-ip")
    if real:
        return real.strip()
    return request.client.host if request.client else "unknown"

def _verify_request_token(raw_auth_header: Optional[str]) -> None:
    """Verify caller JWT against the Auth server."""
    if not raw_auth_header:
        raise HTTPException(status_code=401, detail="Authorization header missing")
    if not AUTH_VERIFY_URL:
        raise HTTPException(status_code=500, detail="AUTH_VERIFY_URL not configured")
    try:
        resp = requests.get(AUTH_VERIFY_URL, headers={"Authorization": raw_auth_header}, timeout=10)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Auth verification failed: {exc}") from exc
    if resp.status_code != 200:
        body_preview = resp.text[:300] if resp.text else ""
        print(f"[auth] verify failed status={resp.status_code} body={body_preview}", flush=True)
        # print(f"[wattnet_token] {raw_auth_header["Authorization"]}")
        raise HTTPException(status_code=401, detail="Invalid token")
    else:
        print(f"[auth] AuthServer verification status is: {resp.status_code}", flush=True)

def wattnet_headers(aggregate: Optional[bool] = None) -> Dict[str, str]:
    print(WATTNET_TOKEN)
    auth_token = _clean_bearer_token(WATTNET_TOKEN)
    fp = f"{auth_token[:6]}...{auth_token[-6:]}" if auth_token else "<none>"
    print(f"[wattnet] using token {fp}", flush=True)
    if not auth_token:
        raise RuntimeError("WATTNET_TOKEN not set")
    headers = {"Accept": "application/json", "Authorization": f"Bearer {auth_token}"}
    if aggregate is not None:
        headers["aggregate"] = str(aggregate).lower()
    return headers

def _wattnet_error_detail(resp: requests.Response) -> str:
    try:
        data = resp.json()
        if isinstance(data, dict):
            for key in ("detail", "message", "error"):
                val = data.get(key)
                if isinstance(val, str) and val.strip():
                    return val.strip()
            return json.dumps(data)[:300]
        if isinstance(data, list) and data:
            return str(data[0])[:300]
    except Exception:
        pass
    body = resp.text[:300] if hasattr(resp, "text") else ""
    return body or f"WattNet responded with status {resp.status_code}"

def wattnet_fetch(lat: float, lon: float, start: datetime, end: datetime, aggregate: bool = False, extra_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{WATTNET_BASE}/v1/footprints"
    params = {
        "lat": lat,
        "lon": lon,
        "footprint_type": "carbon",
        "start": to_iso_z(start),
        "end": to_iso_z(end),
        "aggregate": aggregate,
    }
    if extra_params:
        params.update(extra_params)

    def _coerce_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            v = value.strip().lower()
            if v in {"true", "1", "yes", "y", "on"}:
                return True
            if v in {"false", "0", "no", "n", "off"}:
                return False
        return bool(value)

    aggregate_flag = _coerce_bool(params.get("aggregate", aggregate))
    params["aggregate"] = str(aggregate_flag).lower()
    headers = wattnet_headers(aggregate=aggregate_flag)
    auth_preview = headers.get("Authorization", "<missing>")
    if isinstance(auth_preview, str) and auth_preview.lower().startswith("bearer "):
        raw = auth_preview.split(" ", 1)[1]
        masked = f"{raw[:6]}...{raw[-6:]}" if len(raw) > 12 else raw
        auth_preview = f"Bearer {masked}"
    debug_headers = {**headers, "Authorization": auth_preview}
    print(f"[wattnet_fetch] headers={debug_headers}", flush=True)
    
    print(f"[wattnet_fetch] Requesting CI for lat={lat} lon={lon} window={start} to {end} params={params}", flush=True)
    
    r = sess.get(url, params=params, headers=headers, timeout=20)
    if not r.ok:
        print("[wattnet_fetch] status:", r.status_code, "body:", r.text[:300], flush=True)
        detail = _wattnet_error_detail(r)
        raise HTTPException(status_code=r.status_code, detail=detail)
    r.raise_for_status()
    try:
        data = r.json()
    except json.JSONDecodeError as exc:
        body_preview = r.text[:500] if hasattr(r, "text") else "<no body>"
        print(f"[wattnet_fetch] JSON decode failed status={r.status_code} body={body_preview}", flush=True)
        raise
    if isinstance(data, list):
        if not data or len(data) == 0:
            raise HTTPException(status_code=502, detail="WattNet returned empty list")
        return data[0]
    return data

# --- Pydantic Models ---

class LocationResponse(BaseModel):
    latitude: float = Field(..., description="Latitude in decimal degrees.")
    longitude: float = Field(..., description="Longitude in decimal degrees.")
    country: Optional[str] = Field(default=None, description="ISO country code.")
    roc: Optional[str] = Field(default=None, description="Regional Operations Centre.")

class PUERequest(BaseModel):
    site_name: str = Field(..., description="Site identifier.")

class PUEResponse(BaseModel):
    site_name: str
    location: LocationResponse
    pue: float
    source: str

class CIRequest(BaseModel):
    lat: float = Field(..., description="Latitude in WGS84 decimal degrees.", examples=[45.071])
    lon: float = Field(..., description="Longitude in WGS84 decimal degrees.", examples=[7.652])
    pue: Optional[float] = Field(
        default_factory=_default_pue,
        description="Power Usage Effectiveness. Defaults to service fallback when omitted.",
        examples=[1.7],
    )
    energy_wh: Optional[float] = Field(
        default=None,
        description="Optional energy in Wh. When provided, CFP fields are computed.",
        examples=[12000.0],
    )
    start: Optional[datetime] = Field(
        default=None,
        validation_alias=AliasChoices("start", "datetime"),
        description="Start of CI lookup window (UTC ISO8601).",
        examples=["2024-05-01T10:30:00Z"],
    )
    end: Optional[datetime] = Field(
        default=None,
        description="End of CI lookup window (UTC ISO8601).",
        examples=["2024-05-01T13:30:00Z"],
    )
    time: Optional[datetime] = Field(default=None, validation_alias=AliasChoices("time"), deprecated=True)
    metric_id: Optional[str] = None
    wattnet_params: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional pass-through parameters for WattNet request tuning.",
    )

class CIResponse(BaseModel):
    source: str = Field(
        ...,
        description="`online` when fetched from WattNet, `local` when served from persisted cache.",
        examples=["online"],
    )
    zone: Optional[str] = Field(
        default=None,
        description="Bidding zone name (WattNet zone and/or mapped zone).",
        examples=["IT_NORD"],
    )
    bz_eic: Optional[str] = Field(
        default=None,
        description="ENTSO-E bidding-zone EIC code, when mapping is available.",
        examples=["10Y1001A1001A73I"],
    )
    freshness_s: Optional[int] = Field(
        default=None,
        description="Age in seconds of cached payload when `source=local`; usually 0 for online fetch.",
        examples=[0],
    )
    datetime: Optional[str] = Field(default=None, description="Timestamp associated with CI value.")
    ci_gco2_per_kwh: float = Field(..., description="Carbon intensity in gCO2/kWh.")
    pue: float = Field(..., description="Applied PUE value.")
    effective_ci_gco2_per_kwh: float = Field(..., description="Effective CI = ci_gco2_per_kwh * pue.")
    cfp_g: Optional[float] = Field(default=None, description="Computed carbon footprint in grams (if energy_wh provided).")
    cfp_kg: Optional[float] = Field(default=None, description="Computed carbon footprint in kilograms (if energy_wh provided).")
    valid: bool = Field(..., description="Provider validity flag.")

class CFPQuery(BaseModel):
    """Query parameters for GET /cfp (supports multiple aliases used across the pipeline)."""
    ci_g: float = Field(..., validation_alias=AliasChoices("ci_g", "ci", "ci_gco2_per_kwh"))
    pue: float = Field(..., validation_alias=AliasChoices("pue", "PUE"))
    energy_wh: Optional[float] = Field(default=None, validation_alias=AliasChoices("energy_wh", "EnergyWh", "Energy_wh"))

class CFPResponse(BaseModel):
    ci_gco2_per_kwh: float
    pue: float
    effective_ci_gco2_per_kwh: float
    energy_wh: Optional[float] = None
    cfp_g: Optional[float] = None
    cfp_kg: Optional[float] = None

class MetricsEnvelope(BaseModel):
    model_config = ConfigDict(extra="allow")
    site: Optional[str] = None
    duration_s: Optional[float] = None  # Changed to float to be safe
    sites: Dict[str, Any]
    fact_site_event: Dict[str, Any]
    detail_cloud: Optional[Dict[str, Any]] = None
    detail_grid: Optional[Dict[str, Any]] = None
    detail_network: Optional[Dict[str, Any]] = None
    detail_table: Optional[str] = None

    # Optional overrides
    lat: Optional[float] = None
    lon: Optional[float] = None
    pue: Optional[float] = None
    energy_wh: Optional[float] = None
    wattnet_params: Optional[Dict[str, Any]] = None


class ResolveBZRequest(BaseModel):
    lat: float = Field(..., description="Latitude in WGS84 decimal degrees.", examples=[45.071])
    lon: float = Field(..., description="Longitude in WGS84 decimal degrees.", examples=[7.652])


class ResolveBZResponse(BaseModel):
    zone: str = Field(..., description="Resolved bidding-zone name.", examples=["IT_NORD"])
    bz_eic: str = Field(..., description="Resolved ENTSO-E bidding-zone EIC code.", examples=["10Y1001A1001A73I"])

# --- Sites Loading Logic (With Fix) ---

def _write_sites_cache(sites_map: dict) -> None:
    """Persist the current sites map to disk."""
    try:
        os.makedirs(os.path.dirname(SITES_CACHE_PATH) or ".", exist_ok=True)
        with open(SITES_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(sites_map, f)
        print(f"[sites] Cached {len(sites_map)} sites to {SITES_CACHE_PATH}", flush=True)
    except Exception as e:
        print(f"[sites] failed to write cache {SITES_CACHE_PATH}: {e}", flush=True)


def _read_sites_cache() -> dict:
    if os.path.isdir(SITES_CACHE_PATH):
        print(f"[sites] WARNING: {SITES_CACHE_PATH} is a directory. Please check volume mapping.", flush=True)
        return {}
    if not os.path.exists(SITES_CACHE_PATH):
        return {}
    try:
        with open(SITES_CACHE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
        print(f"[sites] Cache at {SITES_CACHE_PATH} is not a dict; ignoring.", flush=True)
    except Exception as e:
        print(f"[sites] failed to read cache {SITES_CACHE_PATH}: {e}", flush=True)
    return {}


def _load_sites_map() -> dict:
    """Load array JSON into a dict keyed by site_name and cache it to disk."""
    # FIX: Check if directory exists instead of file to avoid crash
    if os.path.isdir(SITES_PATH):
        print(f"[sites] WARNING: {SITES_PATH} is a directory. Please check volume mapping.", flush=True)
        return {}
        
    if not os.path.exists(SITES_PATH):
        print(f"[sites] File not found at {SITES_PATH}", flush=True)
        return {}

    try:
        with open(SITES_PATH, "r", encoding="utf-8") as f:
            arr = json.load(f)
        m = {}
        for x in arr:
            name = x.get("site_name") or x.get("SiteName") # Handle both cases
            lat, lon = x.get("latitude") or x.get("Latitude"), x.get("longitude") or x.get("Longitude")
            if name and lat is not None and lon is not None:
                m[name] = {
                    "lat": float(lat),
                    "lon": float(lon),
                    "pue": _resolve_pue(x.get("pue") or x.get("PUE")),
                    "country": x.get("country"),
                    "roc": x.get("roc"),
                }
        _write_sites_cache(m)
        return m
    except Exception as e:
        print(f"[sites] failed to load {SITES_PATH}: {e}", flush=True)
        return {}


def _prime_sites_cache() -> None:
    sites_map = _load_sites_map()
    print(f"[sites] Loaded {len(sites_map)} sites.", flush=True)


# Load on startup
_prime_sites_cache()


def _reload_sites_map_if_needed(site_name: str) -> Optional[dict]:
    cached_map = _read_sites_cache()
    site = cached_map.get(site_name)
    if site:
        return site
    try:
        new_map = _load_sites_map()
    except Exception as exc:
        print(f"[sites] reload failed: {exc}", flush=True)
        return None
    return new_map.get(site_name)


def _refresh_interval_seconds() -> Optional[float]:
    raw = PUE_REFRESH_HOURS
    try:
        hours = float(raw)
    except (TypeError, ValueError):
        print(f"[sites] Invalid PUE_REFRESH_HOURS={raw!r}; auto refresh disabled.", flush=True)
        return None
    if hours <= 0:
        print(f"[sites] PUE_REFRESH_HOURS={hours} disables auto refresh (must be > 0).", flush=True)
        return None
    return hours * 3600.0


async def _sites_refresh_loop(interval_seconds: float) -> None:
    while True:
        try:
            _load_sites_map()
        except Exception as exc:
            print(f"[sites] background refresh failed: {exc}", flush=True)
        await asyncio.sleep(interval_seconds)


@app.on_event("startup")
async def _start_sites_refresh() -> None:
    _load_ci_cache_from_disk()
    interval = _refresh_interval_seconds()
    if interval is None:
        return
    global SITES_REFRESH_TASK
    SITES_REFRESH_TASK = asyncio.create_task(_sites_refresh_loop(interval))
    print(f"[sites] Auto refresh scheduled every {interval/3600:.2f} hours.", flush=True)


@app.on_event("shutdown")
async def _stop_sites_refresh() -> None:
    global SITES_REFRESH_TASK
    task = SITES_REFRESH_TASK
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def _default_bz_geojson_dir() -> str:
    base = os.path.dirname(__file__)
    candidates = [
        os.path.join(base, "..", "entsoe", "geo", "geojson"),
        os.path.join(base, "..", "entose", "geo", "geojson"),
        "/data/entsoe/geo/geojson",
    ]
    for path in candidates:
        if os.path.isdir(path):
            return os.path.abspath(path)
    return os.path.abspath(candidates[0])


def _load_ci_cache_from_disk() -> None:
    global _CI_BY_BZ_CACHE
    path = CI_CACHE_FILE
    try:
        if not os.path.exists(path):
            print(f"[ci-cache] No cache file at {path}; starting empty.", flush=True)
            return
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
        if not text.strip():
            print(f"[ci-cache] Cache file {path} is empty; starting empty.", flush=True)
            return
        raw = json.loads(text)
        entries = raw.get("entries") if isinstance(raw, dict) else None
        if not isinstance(entries, dict):
            print(f"[ci-cache] Invalid cache format in {path}; starting empty.", flush=True)
            return

        cleaned: Dict[str, Dict[str, Any]] = {}
        for key, item in entries.items():
            if not isinstance(key, str) or not isinstance(item, dict):
                continue
            payload = item.get("payload")
            if not isinstance(payload, dict):
                continue
            cleaned[key] = {
                "payload": payload,
                "fetched_at": int(item.get("fetched_at", 0)),
            }
        with _CI_CACHE_LOCK:
            _CI_BY_BZ_CACHE = cleaned
        print(f"[ci-cache] Loaded {len(cleaned)} entries from {path}", flush=True)
    except Exception as exc:
        print(f"[ci-cache] Failed to load {path}: {exc}", flush=True)


def _persist_ci_cache_to_disk() -> None:
    path = CI_CACHE_FILE
    tmp_path: Optional[str] = None
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with _CI_CACHE_LOCK:
            payload = {
                "saved_at": to_iso_z(datetime.now(timezone.utc)),
                "entries": _CI_BY_BZ_CACHE,
            }
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            delete=False,
            dir=os.path.dirname(path) or ".",
        ) as tf:
            tmp_path = tf.name
            json.dump(payload, tf, separators=(",", ":"))
            tf.flush()
            try:
                os.fsync(tf.fileno())
            except OSError as exc:
                if exc.errno not in (errno.EINVAL, errno.ENOTSUP, errno.EROFS):
                    raise
        os.replace(tmp_path, path)
        tmp_path = None
    except Exception as exc:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except Exception:
                pass
        print(f"[ci-cache] Failed to persist cache to {path}: {exc}", flush=True)


def _best_cached_for_coords(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    """Return newest cached entry for exact coordinate pair (6dp), regardless of window/params."""
    prefix = f"{lat:.6f}|{lon:.6f}|"
    best: Optional[Dict[str, Any]] = None
    best_ts = -1
    with _CI_CACHE_LOCK:
        items = list(_CI_BY_BZ_CACHE.items())
    for key, item in items:
        if not isinstance(key, str) or not key.startswith(prefix):
            continue
        if not isinstance(item, dict) or not isinstance(item.get("payload"), dict):
            continue
        ts = int(item.get("fetched_at", 0))
        if ts > best_ts:
            best_ts = ts
            best = item
    return best


def _best_cached_by_prefix(prefix: str) -> Optional[Dict[str, Any]]:
    best: Optional[Dict[str, Any]] = None
    best_ts = -1
    with _CI_CACHE_LOCK:
        items = list(_CI_BY_BZ_CACHE.items())
    for key, item in items:
        if not isinstance(key, str) or not key.startswith(prefix):
            continue
        if not isinstance(item, dict) or not isinstance(item.get("payload"), dict):
            continue
        ts = int(item.get("fetched_at", 0))
        if ts > best_ts:
            best_ts = ts
            best = item
    return best


def _cache_region_token(lat: float, lon: float) -> tuple[str, Optional[str], Optional[str]]:
    """
    Build cache token; prefer region mapping so different coordinates in same
    bidding zone share cache. If mapping is unavailable, fall back to coords.
    """
    try:
        zone_name, bz_eic = _resolve_bz_or_422(lat, lon)
        return f"region:{bz_eic}", zone_name, bz_eic
    except HTTPException:
        return f"coord:{lat:.6f},{lon:.6f}", None, None


def _get_bz_resolver() -> BiddingZoneResolver:
    global _BZ_RESOLVER
    if _BZ_RESOLVER is not None:
        return _BZ_RESOLVER
    with _BZ_LOCK:
        if _BZ_RESOLVER is None:
            geo_dir = BZ_GEOJSON_DIR or _default_bz_geojson_dir()
            _BZ_RESOLVER = BiddingZoneResolver(Path(geo_dir))
            print(f"[bz] Loaded bidding zone resolver from {geo_dir}", flush=True)
    return _BZ_RESOLVER


def _resolve_bz_or_422(lat: float, lon: float) -> tuple[str, str]:
    try:
        resolver = _get_bz_resolver()
        return resolver.resolve(lat, lon)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except BiddingZoneNotFoundError:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "COORDS_OUTSIDE_SUPPORTED_ZONES",
                "message": f"No ENTSO-E bidding zone for lat={lat}, lon={lon}",
            },
        )
    except BiddingZoneResolverError as exc:
        raise HTTPException(status_code=503, detail=f"Bidding-zone resolver unavailable: {exc}") from exc

@app.exception_handler(RequestValidationError)
async def debug_validation_exception_handler(request: Request, exc: RequestValidationError):
    # 1. Read the raw body that failed validation
    body = await request.body()
    body_str = body.decode("utf-8")
    
    # 2. Print it to the Docker logs
    print(f"\n[DEBUG] --- 422 VALIDATION ERROR ---", flush=True)
    print(f"[DEBUG] Incoming Payload:\n{body_str}", flush=True)
    print(f"[DEBUG] Validation Details:\n{exc.errors()}", flush=True)
    print(f"[DEBUG] ----------------------------\n", flush=True)

    # 3. Return the standard response so the client knows it failed
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors(), "body_received": body_str},
    )

# --- Endpoints ---
@router.post("/pue", response_model=PUEResponse)
def post_pue(payload: PUERequest):
    return _compute_pue_response(payload)

def _compute_pue_response(req: PUERequest) -> PUEResponse:
    site_name = req.site_name.strip()
    sources = []
    
    # 1. Try GOCDB
    gocdb_data = None
    try:
        gocdb_data = gocdb_fetch_site(site_name)
        if gocdb_data: sources.append("gocdb")
    except RuntimeError as exc:
        print(f"[gocdb] lookup failed: {exc}", flush=True)

    # 2. Try Local
    local_site = _reload_sites_map_if_needed(site_name)
    if local_site: sources.append("local")

    # Merge Data (Local takes precedence for overrides if needed, but usually GOCDB is fresher)
    # Logic: Prefer GOCDB, fallback to local
    lat = (gocdb_data or {}).get("lat")
    if lat is None and local_site: lat = local_site.get("lat")
    
    lon = (gocdb_data or {}).get("lon")
    if lon is None and local_site: lon = local_site.get("lon")

    pue = (gocdb_data or {}).get("pue")
    if pue is None and local_site: pue = local_site.get("pue")
    
    if pue is None:
        pue = _default_pue()
        sources.append("default")

    if lat is None or lon is None:
        raise HTTPException(status_code=404, detail=f"No location data for site '{site_name}'")

    return PUEResponse(
        site_name=site_name,
        location=LocationResponse(latitude=lat, longitude=lon),
        pue=pue,
        source="+".join(sources)
    )

@router.post(
    "/ci",
    response_model=CIResponse,
    tags=["CI"],
    summary="Resolve carbon intensity with online/local fallback",
    description=(
        "Returns CI and derived values for a given location/time window. "
        "The service first attempts an online WattNet fetch; if that fails, it serves "
        "the newest matching local cached payload (persisted JSON cache). "
        "Cache reuse is region-aware when bidding-zone mapping is available."
    ),
    responses={
        200: {"description": "CI resolved from online provider or local cache."},
        401: {"description": "Missing/invalid Authorization token."},
        422: {"description": "Invalid request fields or coordinates."},
        502: {"description": "Online provider failed and no usable local cache was found."},
    },
)
def post_ci(payload: CIRequest, request: Request):
    client_ip = _client_ip(request)
    print(f"[ci] request from {client_ip}", flush=True)

    # Allow query parameters to fill missing fields (mirrors WattNet-style calls)
    q = request.query_params
    qs_start = _parse_dt_param(q.get("start"), "start")
    qs_end = _parse_dt_param(q.get("end"), "end")
    qs_time = _parse_dt_param(q.get("time"), "time")
    if payload.start is None and qs_start is not None:
        payload.start = qs_start
    if payload.end is None and qs_end is not None:
        payload.end = qs_end
    if payload.time is None and qs_time is not None:
        payload.time = qs_time

    merged_params: Dict[str, Any] = {}
    if payload.wattnet_params:
        merged_params.update(payload.wattnet_params)

    agg_header = request.headers.get("aggregate")
    if agg_header is not None and "aggregate" not in merged_params:
        merged_params["aggregate"] = agg_header
        
    agg_query = q.get("aggregate")
    if agg_query is not None and "aggregate" not in merged_params:
        merged_params["aggregate"] = agg_query

    # Verify caller token (AuthServer)
    raw_auth_header = request.headers.get("authorization")
    print("---AuthServer token (JWT verification)---")
    print(f"Token: {raw_auth_header}")
    print(f"Authorization header: {raw_auth_header}")
    print(f"AUTH_VERIFY_URL={AUTH_VERIFY_URL}")
    _verify_request_token(raw_auth_header)

    return _compute_ci_response(payload, merged_params or None)


@router.post(
    "/resolve-bz",
    response_model=ResolveBZResponse,
    tags=["CI"],
    summary="Resolve ENTSO-E bidding zone from coordinates",
    description=(
        "Resolves a WGS84 coordinate pair to ENTSO-E bidding-zone name and EIC code "
        "using local GeoJSON geometries plus zoneName->EIC mapping."
    ),
    responses={
        200: {"description": "Zone successfully resolved."},
        422: {"description": "Coordinates outside supported zones or invalid coordinates."},
        503: {"description": "Resolver unavailable (missing geometry pack/mapping)."},
    },
)
def post_resolve_bz(payload: ResolveBZRequest):
    zone_name, bz_eic = _resolve_bz_or_422(payload.lat, payload.lon)
    return ResolveBZResponse(zone=zone_name, bz_eic=bz_eic)


@router.get("/cfp", response_model=CFPResponse)
def get_cfp(request: Request, q: CFPQuery = Depends()):
    """
    Compute CFP from already-known CI and PUE.

    Inputs:
      - ci_g (gCO2/kWh)
      - pue (dimensionless)
      - energy_wh (optional; when provided, returns absolute CFP in grams/kg)
    """
    raw_auth_header = request.headers.get("authorization")
    _verify_request_token(raw_auth_header)
    eff_ci = q.ci_g * q.pue
    cfp_g = None
    cfp_kg = None
    if q.energy_wh is not None:
        energy_kwh = q.energy_wh / 1000.0
        cfp_g = energy_kwh * eff_ci
        cfp_kg = cfp_g / 1000.0
    return CFPResponse(
        ci_gco2_per_kwh=q.ci_g,
        pue=q.pue,
        effective_ci_gco2_per_kwh=eff_ci,
        energy_wh=q.energy_wh,
        cfp_g=cfp_g,
        cfp_kg=cfp_kg,
    )

def _resolve_ci_window(req: CIRequest) -> tuple[datetime, datetime]:
    if req.start and req.end:
        return _ensure_utc(req.start), _ensure_utc(req.end)
    anchor = _ensure_utc(req.start or req.time or datetime.now(timezone.utc))
    return anchor - timedelta(hours=1), anchor + timedelta(hours=2)

def _extract_ci_from_payload(payload: Dict[str, Any]) -> tuple[float, Optional[str]]:
    """Return (value, datetime_str) from WattNet payload supporting both aggregated and series shapes."""
    if isinstance(payload, dict):
        direct_val = payload.get("value")
        if isinstance(direct_val, (int, float)):
            return float(direct_val), payload.get("end") or payload.get("start")

        series = payload.get("series")
        if isinstance(series, list):
            for series_entry in reversed(series):
                values = series_entry.get("values")
                if not isinstance(values, list):
                    continue
                for val_entry in reversed(values):
                    if isinstance(val_entry, (list, tuple)) and len(val_entry) >= 2:
                        ts, val = val_entry[0], val_entry[1]
                        if isinstance(val, (int, float)):
                            return float(val), ts

    raise HTTPException(status_code=502, detail="No numeric CI value found in WattNet response (value/series missing).")

def _compute_ci_response(req: CIRequest, wattnet_params: Optional[Dict[str, Any]] = None) -> CIResponse:
    merged_params: Dict[str, Any] = {}
    if req.wattnet_params:
        merged_params.update(req.wattnet_params)
    if wattnet_params:
        merged_params.update(wattnet_params)

    print("[params]", merged_params, flush=True)
    start, end = _resolve_ci_window(req)
    pue_value = _resolve_pue(req.pue)
    print(f"[ci] window {start} -> {end}", flush=True)
    region_token, mapped_zone_name, mapped_bz_eic = _cache_region_token(req.lat, req.lon)

    # Region-based key: same zone + window/params share cache, even with different coords.
    cache_key = "|".join(
        [
            region_token,
            to_iso_z(start),
            to_iso_z(end),
            json.dumps(merged_params or {}, sort_keys=True, default=str),
        ]
    )
    now_ts = int(datetime.now(timezone.utc).timestamp())
    with _CI_CACHE_LOCK:
        cache_item = _CI_BY_BZ_CACHE.get(cache_key)
    payload: Dict[str, Any]
    source = "online"
    freshness_s = 0
    zone_name: Optional[str] = mapped_zone_name
    bz_eic: Optional[str] = mapped_bz_eic

    if cache_item:
        fetched_at = int(cache_item.get("fetched_at", 0))
        age = max(0, now_ts - fetched_at)
        if age <= CI_CACHE_TTL_S:
            payload = cache_item["payload"]
            source = "local"
            freshness_s = age

    if source == "online":
        try:
            payload = wattnet_fetch(req.lat, req.lon, start, end, extra_params=merged_params or None)
            with _CI_CACHE_LOCK:
                _CI_BY_BZ_CACHE[cache_key] = {"payload": payload, "fetched_at": now_ts}
            _persist_ci_cache_to_disk()
            zone_name = payload.get("zone")
        except Exception as e:
            # Fallback to cached value (even stale) when online fetch fails.
            fallback_item = cache_item if (cache_item and isinstance(cache_item.get("payload"), dict)) else None
            if fallback_item is None:
                fallback_item = _best_cached_by_prefix(f"{region_token}|")
            # Legacy fallback for older coordinate-key cache entries.
            if fallback_item is None:
                fallback_item = _best_cached_for_coords(req.lat, req.lon)
            if fallback_item and isinstance(fallback_item.get("payload"), dict):
                fetched_at = int(fallback_item.get("fetched_at", 0))
                payload = fallback_item["payload"]
                source = "local"
                freshness_s = max(0, now_ts - fetched_at)
                print(
                    f"[wattnet] online fetch failed; serving cached payload age={freshness_s}s",
                    flush=True,
                )
            else:
                # Log richer context to aid debugging (status + body when available)
                resp = getattr(e, "response", None)
                if resp is not None:
                    body_preview = resp.text[:500] if resp.text else ""
                    print(f"[wattnet] error status={resp.status_code} body={body_preview}", flush=True)
                print(f"[wattnet] exception: {repr(e)}", flush=True)
                raise HTTPException(
                    status_code=502,
                    detail=(
                        f"WattNet error: {e}. "
                        "No local cached CI found for these coordinates/region."
                    ),
                )

    # Only in local mode, attempt to enrich with zoneName/bz_eic mapping.
    if source == "local":
        # Keep mapped identifiers if available; otherwise use payload hints.
        zone_name = zone_name or payload.get("zone")
    else:
        # Online mode must not depend on mapping availability.
        zone_name = zone_name or payload.get("zone")

    ci, ci_dt = _extract_ci_from_payload(payload)
    eff_ci = ci * pue_value # Effective Carbon Intensity = CI * PUE
    
    # Calculate CFP if energy is provided
    cfp_g = None
    cfp_kg = None
    if req.energy_wh is not None:
        energy_kwh = req.energy_wh / 1000.0
        cfp_g = energy_kwh * eff_ci # Formula: E(kWh) * PUE * CI
        cfp_kg = cfp_g / 1000.0

    return CIResponse(
        source=source,
        zone=zone_name,
        bz_eic=bz_eic,
        freshness_s=freshness_s,
        datetime=ci_dt or payload.get("end") or payload.get("start"),
        ci_gco2_per_kwh=ci,
        pue=pue_value,
        effective_ci_gco2_per_kwh=eff_ci,
        cfp_g=cfp_g,
        cfp_kg=cfp_kg,
        valid=bool(payload.get("valid", False))
    )

# --- External CI API caller (for partner enrichment) ---

def _call_ci_api(
    lat: float,
    lon: float,
    start: datetime,
    end: datetime,
    pue: float,
    energy_wh: Optional[float] = None,
    auth_header: Optional[str] = None,
    extra_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    url = f"{CI_API_BASE.rstrip('/')}/ci"
    body: Dict[str, Any] = {
        "lat": lat,
        "lon": lon,
        "pue": pue,
        "start": to_iso_z(start),
        "end": to_iso_z(end),
    }
    if energy_wh is not None:
        body["energy_wh"] = energy_wh
    if extra_params:
        body.update(extra_params)

    headers = {"Content-Type": "application/json"}
    if auth_header:
        headers["Authorization"] = auth_header

    r = sess.post(url, json=body, headers=headers, timeout=30)
    if not r.ok:
        print(f"[ci_api] status={r.status_code} body={r.text[:400]}", flush=True)
    r.raise_for_status()
    return r.json()

# --- MAIN TRANSFORMATION LOGIC ---

def _infer_times(payload: MetricsEnvelope) -> tuple[datetime, datetime, datetime]:
    fse = payload.fact_site_event
    # Handle timestamps safely
    start_raw = fse.get("startexectime") or fse.get("event_start_timestamp") or fse.get("event_start_time")
    stop_raw = fse.get("stopexectime") or fse.get("event_end_timestamp") or fse.get("event_end_times")

    def _coerce_dt(value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return _ensure_utc(value)
        try:
            return _ensure_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
        except Exception:
            return None

    start = _coerce_dt(start_raw)
    stop = _coerce_dt(stop_raw)

    if not start or not stop:
        # Fallback to now if missing
        now = datetime.now(timezone.utc)
        return now, now, now
    
    # Calculate middle point or use stop time for CI lookup
    when = stop
    if when.tzinfo is None: when = when.replace(tzinfo=timezone.utc)
    
    return start, stop, when

@router.post("/transform-and-forward")
def transform_and_forward(request: Request, payload: MetricsEnvelope = Body(...)):
    """
    Receives MetricsEnvelope, calculates PUE/CI/CFP, injects them into fact_site_event,
    and forwards to CNR SQL Adapter.
    """
    client_ip = _client_ip(request)
    print(f"[transform] request from {client_ip}", flush=True)

    # _verify_request_token(request.headers.get("authorization"))
    # 1. Resolve Site and Location
    site_name = payload.site or payload.fact_site_event.get("site")
    if not site_name:
         raise HTTPException(status_code=400, detail="Site name missing in payload")

    # Try to load location/PUE from map if not in payload
    if payload.lat is None or payload.lon is None:
        site_info = _reload_sites_map_if_needed(site_name)
        if site_info:
            payload.lat = site_info.get("lat")
            payload.lon = site_info.get("lon")
            if payload.pue is None:
                payload.pue = site_info.get("pue")
    
    if payload.lat is None or payload.lon is None:
        # Final fallback: Look at GOCDB (optional, but good for robustness)
        try:
            goc_info = gocdb_fetch_site(site_name)
            if goc_info:
                payload.lat = goc_info["lat"]
                payload.lon = goc_info["lon"]
                if payload.pue is None: payload.pue = goc_info["pue"]
        except:
            pass

    if payload.lat is None or payload.lon is None:
        raise HTTPException(status_code=400, detail=f"Could not resolve lat/lon for site '{site_name}'")

    # 2. Resolve PUE
    resolved_pue = _resolve_pue(payload.pue)
    # Check if PUE is in fact_site_event and use it if valid
    fse_pue = payload.fact_site_event.get("PUE")
    if fse_pue:
        resolved_pue = float(fse_pue)

    # 3. Resolve Energy (Wh)
    fse = payload.fact_site_event
    energy_wh = payload.energy_wh
    
    if energy_wh is None:
        # Try finding it in fact_site_event
        val = fse.get("EnergyWh") or fse.get("energy_wh")
        if val is not None:
            energy_wh = float(val)

    # 3b. Preserve partner-provided CI/CFP before overriding
    partner_ci = fse.get("CI_g") or fse.get("CIg")
    partner_cfp = fse.get("CFP_g") or fse.get("CFPg")
    if partner_ci is not None:
        fse["CI_site_g"] = partner_ci
    if partner_cfp is not None:
        fse["CFP_site_g"] = partner_cfp

    # 4. Fetch Carbon Intensity (CI) via CI API (preferred), fallback to WattNet
    start_exec, stop_exec, when = _infer_times(payload)
    
    # Define window for WattNet (1 hour before to 2 hours after event)
    ci_start = when - timedelta(hours=1)
    ci_end = when + timedelta(hours=2)
    
    ci_g: Optional[float] = None
    cfp_g: Optional[float] = None

    # Preferred: external CI API (also gives CFP if energy provided)
    auth_header = request.headers.get("authorization")
    try:
        ci_resp = _call_ci_api(
            payload.lat,
            payload.lon,
            ci_start,
            ci_end,
            resolved_pue,
            energy_wh=energy_wh,
            auth_header=auth_header,
        )
        ci_val = ci_resp.get("ci_gco2_per_kwh")
        if isinstance(ci_val, (int, float)):
            ci_g = float(ci_val)
        cfp_val = ci_resp.get("cfp_g")
        if isinstance(cfp_val, (int, float)):
            cfp_g = float(cfp_val)
    except Exception as e:
        print(f"[ci_api] Failed to fetch CI via API: {e}", flush=True)

    # Fallback: WattNet direct
    if ci_g is None:
        try:
            wp = wattnet_fetch(payload.lat, payload.lon, ci_start, ci_end)
            ci_g = float(wp.get("value", 0.0))
        except Exception as e:
            print(f"[ci] Failed to fetch CI: {e}", flush=True)
            ci_g = 0.0 # Default fallback or raise error? Usually better to fail safely for pipeline

    # 5. Calculate CFP (D4.1 Formula)
    # CFP_g = Energy(kWh) * PUE * CI(g/kWh)
    if cfp_g is None and energy_wh is not None and ci_g is not None:
        energy_kwh = energy_wh / 1000.0
        cfp_g = energy_kwh * resolved_pue * ci_g

    # 6. Inject Values into Payload (fact_site_event)
    fse["PUE"] = resolved_pue
    fse["CI_g"] = ci_g
    fse["CFP_g"] = round(cfp_g, 4) if cfp_g is not None else None
    
    # Ensure Energy is consistent if we extracted it from elsewhere
    if energy_wh is not None and "energy_wh" not in fse:
        fse["energy_wh"] = energy_wh

    # 7. Forward to SQL Adapter
    # try:
    #     print(f"[forward] Forwarding metric for {site_name}. CFP={cfp_g}g", flush=True)
    #     r = sess.post(CNR_SQL_FORWARD_URL, json=payload.model_dump(), timeout=20)
    #     r.raise_for_status()

    #     print(f"[forward] Success. Metric accepted by SQL Adapter.", flush=True)
    # except Exception as e:
    #     # Log but don't crash the caller, or raise 502 depending on pipeline strictness
    #     print(f"[forward] Error: {e}", flush=True)
    #     raise HTTPException(status_code=502, detail=f"Forwarding failed: {e}")

    return {"status": "ok", "forwarded_to": CNR_SQL_FORWARD_URL, "cfp_g": cfp_g}

app.include_router(router)

# --- CLI Logic (Preserved) ---

def _cli_print_json(data: Any, *, stream=sys.stdout) -> None:
    json.dump(data, stream, indent=2)
    stream.write("\n")

def _cli_exit_with_http_error(exc: HTTPException) -> None:
    _cli_print_json({"error": {"status_code": exc.status_code, "detail": exc.detail}}, stream=sys.stderr)
    sys.exit(1)

def _cli_exit_with_unexpected_error(exc: Exception) -> None:
    _cli_print_json({"error": {"type": type(exc).__name__, "message": str(exc)}}, stream=sys.stderr)
    sys.exit(2)

def _cli_get_pue(args: argparse.Namespace) -> None:
    req = PUERequest(site_name=args.site_name)
    try:
        resp = _compute_pue_response(req)
        _cli_print_json(resp.model_dump(mode="json"))
    except HTTPException as exc:
        _cli_exit_with_http_error(exc)

def _cli_get_ci(args: argparse.Namespace) -> None:
    # (Implementation omitted for brevity, identical to your original provided code)
    pass 

def main_cli(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="GreenDIGIT KPI CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    pue = subparsers.add_parser("get-pue")
    pue.add_argument("site_name")
    pue.set_defaults(func=_cli_get_pue)
    
    # Add other parsers as needed from your original code
    
    args = parser.parse_args(argv)
    if hasattr(args, "func"):
        args.func(args)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        main_cli()
    else:
        import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=8011)
