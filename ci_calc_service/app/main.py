import argparse
import json
import os
import sys
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from fastapi import Body, FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Any, Dict, Optional
from pymongo import MongoClient

APP_DESCRIPTION = (
    "Service providing GreenDIGIT KPIs. It retrieves location information from "
    "GOC DB, queries WattNet for carbon intensity, and exposes helper endpoints "
    "used by the CIM pipeline."
)

app = FastAPI(title="GreenDIGIT KPI Service", description=APP_DESCRIPTION)

WATTNET_BASE = os.getenv("WATTNET_BASE") or os.getenv("WATTPRINT_BASE", "https://api.wattnet.eu")
WATTNET_TOKEN = os.getenv("WATTNET_TOKEN") or os.getenv("WATTPRINT_TOKEN")

RETAIN_MONGO_URI = os.getenv("RETAIN_MONGO_URI")
RETAIN_DB_NAME   = os.getenv("RETAIN_DB_NAME", "ci-retainment-db")
RETAIN_COLL      = os.getenv("RETAIN_COLL", "pending_ci")

CNR_SQL_FORWARD_URL = os.getenv("CNR_SQL_FORWARD_URL", "http://sql-cnr-adapter:8033/cnr-sql-service")
PUE_DEFAULT = os.getenv("PUE_DEFAULT", "1.7")

SITES_PATH = os.environ.get("SITES_JSON", "/data/sites_latlngpue.json")  # volume mount
SITES_MAP: dict[str, dict] = {}  # site_name -> {lat, lon, pue}

sess = requests.Session()

GOCDB_BASE = os.getenv("GOCDB_BASE", "https://goc.egi.eu/gocdbpi")
GOCDB_SCOPE = os.getenv("GOCDB_SCOPE")
GOCDB_TOKEN = os.getenv("GOCDB_TOKEN") or os.getenv("GOCDB_OAUTH_TOKEN")
GOCDB_CERT = os.getenv("GOCDB_CERT")
GOCDB_KEY = os.getenv("GOCDB_KEY")
GOCDB_TIMEOUT = float(os.getenv("GOCDB_TIMEOUT", "20"))

goc_sess = requests.Session()
goc_sess.headers["Accept"] = "application/xml"
if GOCDB_TOKEN:
    goc_sess.headers["Authorization"] = f"Bearer {GOCDB_TOKEN}"
if GOCDB_CERT:
    goc_sess.cert = (GOCDB_CERT, GOCDB_KEY) if GOCDB_KEY else GOCDB_CERT

PUE_FALLBACK = 1.7


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
    return f"{GOCDB_BASE.rstrip('/')}/public/"


def _text_or_none(el: Optional[ET.Element], path: str) -> Optional[str]:
    if el is None:
        return None
    found = el.find(path)
    if found is not None and found.text:
        return found.text.strip()
    return None


def _extract_pue_from_extensions(site_el: ET.Element) -> Optional[float]:
    extensions = site_el.find("./EXTENSIONS")
    if extensions is None:
        return None
    for ext in extensions.findall("./EXTENSION"):
        name = _text_or_none(ext, "./EXTENSION_NAME")
        if name and name.strip().lower() == "pue":
            value = _text_or_none(ext, "./EXTENSION_VALUE")
            if value is None:
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
    return None


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
    pue = _extract_pue_from_extensions(site_el)

    return {
        "lat": lat,
        "lon": lon,
        "country": country,
        "roc": roc,
        "pue": pue,
    }


def to_iso_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def wattnet_headers() -> Dict[str, str]:
    if not WATTNET_TOKEN:
        raise RuntimeError("WATTNET_TOKEN not set")
    return {"Accept": "application/json", "Authorization": f"Bearer {WATTNET_TOKEN}"}

def wattnet_fetch(lat: float, lon: float, start: datetime, end: datetime, aggregate: bool = True, extra_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{WATTNET_BASE}/v1/footprints"
    params = {
        "lat": lat,
        "lon": lon,
        "footprint_type": "carbon",
        "start": to_iso_z(start),
        "end": to_iso_z(end),
        "aggregate": str(aggregate).lower(),
    }
    if extra_params:
        params.update(extra_params)
    headers = wattnet_headers()
    print("[wattnet_fetch] URL:", url, "params:", params, flush=True)
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

class LocationResponse(BaseModel):
    latitude: float
    longitude: float
    country: Optional[str] = None
    roc: Optional[str] = None


class PUERequest(BaseModel):
    site_name: str


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
    time: Optional[datetime] = None
    metric_id: Optional[str] = None
    wattnet_params: Optional[Dict[str, Any]] = None

class CIResponse(BaseModel):
    source: str
    zone: Optional[str]
    datetime: Optional[str]
    ci_gco2_per_kwh: float
    pue: float
    effective_ci_gco2_per_kwh: float
    cfp_g: Optional[float]
    cfp_kg: Optional[float]
    valid: bool
    
class MetricsEnvelope(BaseModel):
    # top-level convenience fields
    site: Optional[str] = None
#     ts: Optional[datetime] = None
    duration_s: Optional[int] = None

    # original document parts (kept as free-form dicts to avoid tight coupling)
    sites: Dict[str, Any]
    fact_site_event: Dict[str, Any]
    detail_cloud: Dict[str, Any]

    # for CI request (must be present or resolvable)
    lat: Optional[float] = None
    lon: Optional[float] = None

    # optional inputs to CI calculation
    pue: Optional[float] = None
    energy_wh: Optional[float] = None
    wattnet_params: Optional[Dict[str, Any]] = None
    
def _load_sites_map() -> dict:
    """Load array JSON into a dict keyed by site_name."""
    with open(SITES_PATH, "r", encoding="utf-8") as f:
        arr = json.load(f)
    m = {}
    for x in arr:
        name = x.get("site_name")
        lat, lon = x.get("latitude"), x.get("longitude")
        if name and lat is not None and lon is not None:
            m[name] = {
                "lat": float(lat),
                "lon": float(lon),
                "pue": _resolve_pue(x.get("pue")),
            }
    return m

# load once at startup
try:
    SITES_MAP = _load_sites_map()
    print(f"[sites] Loaded sites into the SITES_MAP variable.")
except Exception as e:
    print(f"[sites] failed to load {SITES_PATH}: {e}", flush=True)
    SITES_MAP = {}

def maybe_retain_invalid(ci_payload: Dict[str, Any], req: CIRequest, start: datetime, end: datetime):
    if not RETAIN_MONGO_URI:
        return
    try:
        cli = MongoClient(RETAIN_MONGO_URI, appname="ci-calc-get-ci", serverSelectionTimeoutMS=3000)
        coll = cli[RETAIN_DB_NAME][RETAIN_COLL]
        coll.insert_one({
            "metric_id": req.metric_id,
            "provider": "wattnet",
            "creation_time": datetime.now(timezone.utc),
            "request_time": [start, end],
            "lat": req.lat,
            "lon": req.lon,
            "pue": _resolve_pue(req.pue),
            "energy_wh": req.energy_wh,
            "wattnet_params": req.wattnet_params,
            "raw_response": ci_payload,
            "valid": bool(ci_payload.get("valid", False)),
        })
    except Exception as e:
        print("[retain] insert failed:", e, flush=True)


def _reload_sites_map_if_needed(site_name: str) -> Optional[dict]:
    site = SITES_MAP.get(site_name)
    if site:
        return site
    try:
        SITES_MAP.update(_load_sites_map())
    except Exception as exc:
        print(f"[sites] reload failed: {exc}", flush=True)
        return None
    return SITES_MAP.get(site_name)


@app.post("/get-pue", response_model=PUEResponse)
def get_pue(req: PUERequest):
    site_name = req.site_name.strip()
    if not site_name:
        raise HTTPException(status_code=400, detail="site_name is required")

    sources: list[str] = []
    gocdb_data: Optional[Dict[str, Any]] = None

    try:
        gocdb_data = gocdb_fetch_site(site_name)
    except RuntimeError as exc:
        print(f"[gocdb] lookup failed for '{site_name}': {exc}", flush=True)
        gocdb_data = None

    if gocdb_data:
        sources.append("gocdb")

    local_site = _reload_sites_map_if_needed(site_name)
    if local_site:
        sources.append("local")

    lat: Optional[float] = None
    lon: Optional[float] = None
    country: Optional[str] = None
    roc: Optional[str] = None
    pue: Optional[float] = None

    if gocdb_data:
        lat = gocdb_data.get("lat")
        lon = gocdb_data.get("lon")
        country = gocdb_data.get("country")
        roc = gocdb_data.get("roc")
        pue = gocdb_data.get("pue")

    if local_site:
        if lat is None and local_site.get("lat") is not None:
            lat = float(local_site.get("lat"))
        if lon is None and local_site.get("lon") is not None:
            lon = float(local_site.get("lon"))
        if country is None and local_site.get("country"):
            country = local_site.get("country")
        if roc is None and local_site.get("roc"):
            roc = local_site.get("roc")
        if pue is None and local_site.get("pue") is not None:
            pue = float(local_site.get("pue"))

    if pue is None:
        pue = _default_pue()
        sources.append("default")

    if lat is None or lon is None or pue is None:
        raise HTTPException(status_code=404, detail=f"No location/PUE data for site '{site_name}'")

    location = LocationResponse(latitude=lat, longitude=lon, country=country, roc=roc)
    source = "+".join(dict.fromkeys(sources)) if sources else "unknown"

    return PUEResponse(site_name=site_name, location=location, pue=pue, source=source)


@app.post("/get-ci", response_model=CIResponse)
def get_ci(req: CIRequest):
    when  = req.time or datetime.now(timezone.utc)
    start = when - timedelta(hours=1)
    end   = when + timedelta(hours=2)
    pue_value = _resolve_pue(req.pue)
    try:
        payload = wattnet_fetch(req.lat, req.lon, start, end, aggregate=True, extra_params=req.wattnet_params)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"WattNet error: {e}")
    ci = float(payload["value"])
    dt_str = payload.get("end") or payload.get("start")
    eff = ci * pue_value
    cfp_g = eff * req.energy_wh if req.energy_wh is not None else None
    cfp_kg = (cfp_g / 1000.0) if cfp_g is not None else None
    valid_flag = bool(payload.get("valid", False))
    if not valid_flag:
        maybe_retain_invalid(payload, req, start, end)
    return CIResponse(
        source="wattnet",
        zone=payload.get("zone"),
        datetime=dt_str,
        ci_gco2_per_kwh=ci,
        pue=pue_value,
        effective_ci_gco2_per_kwh=eff,
        cfp_g=cfp_g,
        cfp_kg=cfp_kg,
        valid=valid_flag,
    )

@app.post("/ci-valid", response_model=CIResponse)
def compute_ci_valid(req: CIRequest):
    when  = req.time or datetime.now(timezone.utc)
    start = when - timedelta(hours=1)
    end   = when + timedelta(hours=2)
    pue_value = _resolve_pue(req.pue)
    try:
        payload = wattnet_fetch(req.lat, req.lon, start, end, aggregate=True, extra_params=req.wattnet_params)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"WattNet error: {e}")
    ci = float(payload["value"])
    dt_str = payload.get("end") or payload.get("start")
    eff = ci * pue_value
    cfp_g = eff * req.energy_wh if req.energy_wh is not None else None
    cfp_kg = (cfp_g / 1000.0) if cfp_g is not None else None
    return CIResponse(
        source="wattnet",
        zone=payload.get("zone"),
        datetime=dt_str,
        ci_gco2_per_kwh=ci,
        pue=pue_value,
        effective_ci_gco2_per_kwh=eff,
        cfp_g=cfp_g,
        cfp_kg=cfp_kg,
        valid=bool(payload.get("valid", False)),
    )

def _infer_times(payload: MetricsEnvelope) -> tuple[datetime, datetime, datetime]:
    """Return (start_exec, stop_exec, when_for_ci)."""
    fse = payload.fact_site_event
    # parse exec window
    start = datetime.fromisoformat(fse["startexectime"].replace("Z", "+00:00"))
    stop  = datetime.fromisoformat(fse["stopexectime"].replace("Z", "+00:00"))
#     # CI 'when' – prefer top-level ts, else event_end_timestamp, else stop time
#     if payload.ts:
#         when = payload.ts
    if "event_end_timestamp" in fse:
        when = datetime.fromisoformat(fse["event_end_timestamp"].replace("Z", "+00:00"))
    else:
        when = stop
    # normalise to UTC and strip microseconds
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return (start, stop, when.astimezone(timezone.utc).replace(microsecond=0))

@app.post("/transform-and-forward")
def transform_and_forward(payload: MetricsEnvelope = Body(...)):
    site_name = payload.site or payload.fact_site_event.get("site")
    if (payload.lat is None or payload.lon is None) and site_name:
        site = SITES_MAP.get(site_name)
        if not site:
            try:
                SITES_MAP.update(_load_sites_map())
                site = SITES_MAP.get(site_name)
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Failed to reload sites: {e}")
        if not site:
            raise HTTPException(status_code=400, detail=f"No mapping for site '{site_name}' in {SITES_PATH}")
        payload.lat = site["lat"]
        payload.lon = site["lon"]
        # prefer PUE from mapping unless already provided
        if payload.fact_site_event.get("PUE") is None and payload.__dict__.get("pue") is None:
            pass

    if payload.lat is None or payload.lon is None:
        raise HTTPException(status_code=400, detail="lat and lon are required or must be resolvable from 'site'")

    start_exec, stop_exec, when = _infer_times(payload)
    if payload.duration_s is None:
        payload.duration_s = int((stop_exec - start_exec).total_seconds())
        
    fse = payload.fact_site_event
    e_wh = payload.energy_wh if payload.energy_wh is not None else fse.get("energy_wh")
    if e_wh is not None:
        try:
            e_wh = float(e_wh)
        except Exception:
            e_wh = None

    # fallback derivations (optional)
    if e_wh is None and fse.get("energy_kwh") is not None:
        e_wh = float(fse["energy_kwh"]) * 1000.0
    if e_wh is None and fse.get("power_w") is not None and payload.duration_s is not None:
        # power (W) * duration (s) -> Joules / 3600 -> Wh
        e_wh = float(fse["power_w"]) * float(payload.duration_s) / 3600.0
    
    payload.energy_wh = e_wh
    if e_wh is not None:
        fse["energy_wh"] = e_wh

    site_pue = None
    site_name = payload.site or payload.fact_site_event.get("site")
    if site_name and site_name in SITES_MAP:
        site_pue = SITES_MAP[site_name].get("pue")

    fact_pue = payload.fact_site_event.get("PUE")
    resolved_pue = _resolve_pue(fact_pue if fact_pue is not None else site_pue)
    ci_req = CIRequest(
        lat=payload.lat,
        lon=payload.lon,
        pue=resolved_pue,
        energy_wh=payload.energy_wh,
        time=when,
        metric_id=str(payload.detail_cloud.get("event_id", "")) or payload.detail_cloud.get("execunitid"),
        wattnet_params=payload.wattnet_params,
    )

    start = when - timedelta(hours=1)
    end   = when + timedelta(hours=2)
    try:
        wp = wattnet_fetch(ci_req.lat, ci_req.lon, start, end, aggregate=True, extra_params=ci_req.wattnet_params)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"WattNet error: {e}")

    ci = float(wp["value"])
    ci_req_pue = _resolve_pue(ci_req.pue)
    eff_ci = ci * ci_req_pue
    
    energy_kwh = (ci_req.energy_wh / 1000.0) if ci_req.energy_wh is not None else None
    cfp_g = eff_ci * energy_kwh if energy_kwh is not None else None
    
    print(f"[ci] ci={ci} pue={ci_req.pue} eff_ci={eff_ci} "
      f"energy_wh={ci_req.energy_wh} energy_kwh={(ci_req.energy_wh/1000.0) if ci_req.energy_wh else None} "
      f"-> cfp_g={cfp_g}", flush=True)

    fse = payload.fact_site_event
    
    fse["PUE"]  = ci_req_pue
    fse["CI_g"] = ci
    if cfp_g is not None:
        fse["CFP_g"] = cfp_g

    try:
        r = sess.post(CNR_SQL_FORWARD_URL, json=payload.dict(), timeout=20)
        if not r.ok:
            print("[forward] status:", r.status_code, "body:", r.text[:300], flush=True)
        r.raise_for_status()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Forwarding to CNR SQL service failed: {e}")

    return {"status": "ok", "forwarded_to": CNR_SQL_FORWARD_URL}


def _cli_print_json(data: Any, *, stream=sys.stdout) -> None:
    json.dump(data, stream, indent=2)
    stream.write("\n")


def _cli_exit_with_http_error(exc: HTTPException) -> None:
    _cli_print_json({"error": {"status_code": exc.status_code, "detail": exc.detail}}, stream=sys.stderr)
    sys.exit(1)


def _cli_exit_with_unexpected_error(exc: Exception) -> None:
    _cli_print_json(
        {"error": {"type": type(exc).__name__, "message": str(exc)}},
        stream=sys.stderr,
    )
    sys.exit(2)


def _cli_parse_time(value: str) -> datetime:
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        dt = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid ISO 8601 datetime '{value}': {exc}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _cli_parse_wattnet_params(raw: str) -> Dict[str, Any]:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON for WattNet params: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ValueError("WattNet params must decode to a JSON object (dictionary).")
    return parsed


def _cli_get_pue(args: argparse.Namespace) -> None:
    req = PUERequest(site_name=args.site_name)
    try:
        resp = get_pue(req)
    except HTTPException as exc:
        _cli_exit_with_http_error(exc)
    except Exception as exc:
        _cli_exit_with_unexpected_error(exc)
    else:
        _cli_print_json(resp.model_dump(mode="json"))


def _cli_get_ci(args: argparse.Namespace) -> None:
    ci_kwargs: Dict[str, Any] = {
        "lat": args.lat,
        "lon": args.lon,
    }
    if args.pue is not None:
        ci_kwargs["pue"] = args.pue
    if args.energy_wh is not None:
        ci_kwargs["energy_wh"] = args.energy_wh
    if args.time:
        try:
            ci_kwargs["time"] = _cli_parse_time(args.time)
        except ValueError as exc:
            _cli_exit_with_unexpected_error(exc)
    if args.metric_id:
        ci_kwargs["metric_id"] = args.metric_id
    if args.wattnet_params:
        try:
            ci_kwargs["wattnet_params"] = _cli_parse_wattnet_params(args.wattnet_params)
        except ValueError as exc:
            _cli_exit_with_unexpected_error(exc)

    req = CIRequest(**ci_kwargs)
    try:
        resp = get_ci(req)
    except HTTPException as exc:
        _cli_exit_with_http_error(exc)
    except Exception as exc:
        _cli_exit_with_unexpected_error(exc)
    else:
        _cli_print_json(resp.model_dump(mode="json"))


def main_cli(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Interact with the GreenDIGIT KPI service without running the HTTP server.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    pue_parser = subparsers.add_parser("get-pue", help="Fetch PUE and location metadata for a site.")
    pue_parser.add_argument("site_name", help="Site name as listed in GOC DB or the local sites map.")
    pue_parser.set_defaults(func=_cli_get_pue)

    ci_parser = subparsers.add_parser("get-ci", help="Fetch carbon intensity for a location and optional energy usage.")
    ci_parser.add_argument("--lat", type=float, required=True, help="Latitude in decimal degrees.")
    ci_parser.add_argument("--lon", type=float, required=True, help="Longitude in decimal degrees.")
    ci_parser.add_argument(
        "--pue",
        type=float,
        help="Power Usage Effectiveness to apply. Defaults to 1.7 when omitted.",
    )
    ci_parser.add_argument(
        "--energy-wh",
        type=float,
        help="Energy consumed in watt-hours. Used to derive CFP if provided.",
    )
    ci_parser.add_argument(
        "--time",
        help="ISO 8601 timestamp (UTC) for the query window, for example 2024-01-01T00:00:00Z.",
    )
    ci_parser.add_argument("--metric-id", help="Optional metric identifier propagated to retainment.")
    ci_parser.add_argument(
        "--wattnet-params",
        help="Additional WattNet query parameters as a JSON object string.",
    )
    ci_parser.set_defaults(func=_cli_get_ci)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main_cli()
