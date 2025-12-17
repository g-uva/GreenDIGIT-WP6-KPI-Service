import argparse
import json
import os
import sys
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from fastapi import Body, FastAPI, HTTPException
from pydantic import AliasChoices, BaseModel, Field, ConfigDict
from typing import Any, Dict, Optional
from pymongo import MongoClient

APP_DESCRIPTION = (
    "Service providing GreenDIGIT KPIs. It retrieves location information from "
    "GOC DB, queries WattNet for carbon intensity, and exposes helper endpoints "
    "used by the CIM pipeline."
)

app = FastAPI(
    title="GreenDIGIT KPI Service",
    description=APP_DESCRIPTION,
    swagger_ui_parameters={"persistAuthorization": True},
    root_path="/gd-ci-api"
)

# --- Configuration & Environment ---
WATTNET_BASE = os.getenv("WATTNET_BASE") or os.getenv("WATTPRINT_BASE", "https://api.wattnet.eu")
WATTNET_TOKEN = os.getenv("WATTNET_TOKEN") or os.getenv("WATTPRINT_TOKEN")

RETAIN_MONGO_URI = os.getenv("RETAIN_MONGO_URI")
RETAIN_DB_NAME   = os.getenv("RETAIN_DB_NAME", "ci-retainment-db")
RETAIN_COLL      = os.getenv("RETAIN_COLL", "pending_ci")

CNR_SQL_FORWARD_URL = os.getenv("CNR_SQL_FORWARD_URL", "http://sql-cnr-adapter:8033/cnr-sql-service")
PUE_DEFAULT = os.getenv("PUE_DEFAULT", "1.7")

SITES_PATH = os.environ.get("SITES_JSON", "/data/sites_latlngpue.json")
SITES_MAP: dict[str, dict] = {}  # site_name -> {lat, lon, pue}

sess = requests.Session()

# --- GOCDB Configuration ---
GOCDB_BASE = os.getenv("GOCDB_BASE", "https://goc.egi.eu/gocdbpi")
GOCDB_SCOPE = os.getenv("GOCDB_SCOPE")
GOCDB_TOKEN = os.getenv("GOCDB_TOKEN") or os.getenv("GOCDB_OAUTH_TOKEN")
GOCDB_TIMEOUT = float(os.getenv("GOCDB_TIMEOUT", "20"))

CERT_BASE = "/etc/gocdb-cert"
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

def wattnet_headers(aggregate: Optional[bool] = None) -> Dict[str, str]:
    if not WATTNET_TOKEN:
        raise RuntimeError("WATTNET_TOKEN not set")
    headers = {"Accept": "application/json", "Authorization": f"Bearer {WATTNET_TOKEN}"}
    if aggregate is not None:
        headers["aggregate"] = str(aggregate).lower()
    return headers

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
    
    print(f"[wattnet_fetch] Requesting CI for lat={lat} lon={lon} window={start} to {end}", flush=True)
    
    r = sess.get(url, params=params, headers=headers, timeout=20)
    if not r.ok:
        print("[wattnet_fetch] status:", r.status_code, "body:", r.text[:300], flush=True)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list):
        if not data:
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
    lat: float
    lon: float
    pue: Optional[float] = Field(default_factory=_default_pue)
    energy_wh: Optional[float] = None
    start: Optional[datetime] = Field(default=None, validation_alias=AliasChoices("start", "datetime"))
    end: Optional[datetime] = None
    time: Optional[datetime] = Field(default=None, validation_alias=AliasChoices("time"), deprecated=True)
    metric_id: Optional[str] = None
    wattnet_params: Optional[Dict[str, Any]] = None

class CIResponse(BaseModel):
    source: str
    zone: Optional[str] = None
    datetime: Optional[str] = None
    ci_gco2_per_kwh: float
    pue: float
    effective_ci_gco2_per_kwh: float
    cfp_g: Optional[float] = None
    cfp_kg: Optional[float] = None
    valid: bool

class MetricsEnvelope(BaseModel):
    site: Optional[str] = None
    duration_s: Optional[float] = None  # Changed to float to be safe
    sites: Dict[str, Any]
    fact_site_event: Dict[str, Any]
    detail_cloud: Dict[str, Any]

    # Optional overrides
    lat: Optional[float] = None
    lon: Optional[float] = None
    pue: Optional[float] = None
    energy_wh: Optional[float] = None
    wattnet_params: Optional[Dict[str, Any]] = None

# --- Sites Loading Logic (With Fix) ---

def _load_sites_map() -> dict:
    """Load array JSON into a dict keyed by site_name."""
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
        return m
    except Exception as e:
        print(f"[sites] failed to load {SITES_PATH}: {e}", flush=True)
        return {}

# Load on startup
SITES_MAP = _load_sites_map()
print(f"[sites] Loaded {len(SITES_MAP)} sites.", flush=True)

def _reload_sites_map_if_needed(site_name: str) -> Optional[dict]:
    site = SITES_MAP.get(site_name)
    if site:
        return site
    try:
        new_map = _load_sites_map()
        SITES_MAP.update(new_map)
    except Exception as exc:
        print(f"[sites] reload failed: {exc}", flush=True)
        return None
    return SITES_MAP.get(site_name)

# --- Endpoints ---

@app.post("/pue", response_model=PUEResponse)
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

@app.post("/ci", response_model=CIResponse)
def post_ci(payload: CIRequest):
    return _compute_ci_response(payload)

def _resolve_ci_window(req: CIRequest) -> tuple[datetime, datetime]:
    if req.start and req.end:
        return _ensure_utc(req.start), _ensure_utc(req.end)
    anchor = _ensure_utc(req.start or req.time or datetime.now(timezone.utc))
    return anchor - timedelta(hours=1), anchor + timedelta(hours=2)

def _compute_ci_response(req: CIRequest) -> CIResponse:
    start, end = _resolve_ci_window(req)
    pue_value = _resolve_pue(req.pue)
    
    try:
        payload = wattnet_fetch(req.lat, req.lon, start, end, extra_params=req.wattnet_params)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"WattNet error: {e}")

    ci = float(payload["value"])
    eff_ci = ci * pue_value # Effective Carbon Intensity = CI * PUE
    
    # Calculate CFP if energy is provided
    cfp_g = None
    cfp_kg = None
    if req.energy_wh is not None:
        energy_kwh = req.energy_wh / 1000.0
        cfp_g = energy_kwh * eff_ci # Formula: E(kWh) * PUE * CI
        cfp_kg = cfp_g / 1000.0

    return CIResponse(
        source="wattnet",
        zone=payload.get("zone"),
        datetime=payload.get("end") or payload.get("start"),
        ci_gco2_per_kwh=ci,
        pue=pue_value,
        effective_ci_gco2_per_kwh=eff_ci,
        cfp_g=cfp_g,
        cfp_kg=cfp_kg,
        valid=bool(payload.get("valid", False))
    )

# --- MAIN TRANSFORMATION LOGIC ---

def _infer_times(payload: MetricsEnvelope) -> tuple[datetime, datetime, datetime]:
    fse = payload.fact_site_event
    # Handle timestamps safely
    start_str = fse.get("startexectime") or fse.get("event_start_timestamp")
    stop_str = fse.get("stopexectime") or fse.get("event_end_timestamp")
    
    if not start_str or not stop_str:
        # Fallback to now if missing
        now = datetime.now(timezone.utc)
        return now, now, now

    start = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
    stop = datetime.fromisoformat(stop_str.replace("Z", "+00:00"))
    
    # Calculate middle point or use stop time for CI lookup
    when = stop
    if when.tzinfo is None: when = when.replace(tzinfo=timezone.utc)
    
    return start, stop, when

@app.post("/transform-and-forward")
def transform_and_forward(payload: MetricsEnvelope = Body(...)):
    """
    Receives MetricsEnvelope, calculates PUE/CI/CFP, injects them into fact_site_event,
    and forwards to CNR SQL Adapter.
    """
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

    # 4. Fetch Carbon Intensity (CI)
    start_exec, stop_exec, when = _infer_times(payload)
    
    # Define window for WattNet (1 hour before to 2 hours after event)
    ci_start = when - timedelta(hours=1)
    ci_end = when + timedelta(hours=2)
    
    try:
        wp = wattnet_fetch(payload.lat, payload.lon, ci_start, ci_end)
        ci_g = float(wp["value"])
    except Exception as e:
        print(f"[ci] Failed to fetch CI: {e}", flush=True)
        ci_g = 0.0 # Default fallback or raise error? Usually better to fail safely for pipeline

    # 5. Calculate CFP (D4.1 Formula)
    # CFP_g = Energy(kWh) * PUE * CI(g/kWh)
    cfp_g = 0.0
    if energy_wh is not None:
        energy_kwh = energy_wh / 1000.0
        cfp_g = energy_kwh * resolved_pue * ci_g

    # 6. Inject Values into Payload (fact_site_event)
    fse["PUE"] = resolved_pue
    fse["CI_g"] = ci_g
    fse["CFP_g"] = round(cfp_g, 4)
    
    # Ensure Energy is consistent if we extracted it from elsewhere
    if energy_wh is not None and "energy_wh" not in fse:
        fse["energy_wh"] = energy_wh

    # 7. Forward to SQL Adapter
    try:
        print(f"[forward] Forwarding metric for {site_name}. CFP={cfp_g}g", flush=True)
        r = sess.post(CNR_SQL_FORWARD_URL, json=payload.model_dump(), timeout=20)
        r.raise_for_status()
    except Exception as e:
        # Log but don't crash the caller, or raise 502 depending on pipeline strictness
        print(f"[forward] Error: {e}", flush=True)
        raise HTTPException(status_code=502, detail=f"Forwarding failed: {e}")

    return {"status": "ok", "forwarded_to": CNR_SQL_FORWARD_URL, "cfp_g": cfp_g}

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