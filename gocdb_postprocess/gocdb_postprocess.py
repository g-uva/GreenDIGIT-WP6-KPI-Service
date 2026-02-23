#!/usr/bin/env python3
"""
gocdb_postprocess.py
Enrich site JSON with GOCDB lat/lng/PUE; fallback to local coords + static PUE.
"""

import json
import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests

STATIC_PUE = 1.7
SITE_COORDS_PATH = Path(__file__).resolve().parent.parent / "site_coords.json"
# Country centroids produced by get_country_coordinates/main.py
COUNTRY_LATLNG_PATH = Path(__file__).resolve().parent.parent / "output" / "country_latlng.json"

# --- GOCDB configuration (mirror service defaults) ---
GOCDB_BASE = os.getenv("GOCDB_BASE", "https://goc.egi.eu/gocdbpi")
GOCDB_SCOPE = os.getenv("GOCDB_SCOPE")
GOCDB_TOKEN = os.getenv("GOCDB_TOKEN") or os.getenv("GOCDB_OAUTH_TOKEN")
GOCDB_TIMEOUT = float(os.getenv("GOCDB_TIMEOUT", "20"))

CONTAINER_CERT_BASE = "/etc/gocdb-cert"
CERT_BASE = ""
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


def _gocdb_endpoint() -> str:
    suffix = "private" if (GOCDB_TOKEN or CERT_PATH or KEY_PATH) else "public"
    return f"{GOCDB_BASE.rstrip('/')}/{suffix}/"


def _new_session() -> requests.Session:
    s = requests.Session()
    s.headers["Accept"] = "application/xml"
    if GOCDB_TOKEN:
        s.headers["Authorization"] = f"Bearer {GOCDB_TOKEN}"
    if CERT_PATH:
        s.cert = (CERT_PATH, KEY_PATH) if KEY_PATH else CERT_PATH
    if CA_PATH:
        s.verify = CA_PATH
    return s


def _text(el: Optional[ET.Element], path: str) -> Optional[str]:
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
        name = _text(ext, "./EXTENSION_NAME") or _text(ext, "./KEY")
        if name and name.strip().lower() == "pue":
            val_txt = _text(ext, "./EXTENSION_VALUE") or _text(ext, "./VALUE")
            if val_txt:
                try:
                    return float(val_txt)
                except (TypeError, ValueError):
                    continue
    return None


def fetch_site_from_gocdb(session: requests.Session, site_name: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Return site info dict or (None, error)."""
    url = _gocdb_endpoint()
    params = {"method": "get_site", "sitename": site_name}
    if GOCDB_SCOPE:
        params["scope"] = GOCDB_SCOPE
    try:
        r = session.get(url, params=params, timeout=GOCDB_TIMEOUT)
    except Exception as exc:
        return None, f"request:{exc}"

    if r.status_code in (401, 403):
        return None, f"unauthorized:{r.status_code}"
    if r.status_code == 404:
        return None, "not_found"
    try:
        r.raise_for_status()
    except Exception as exc:
        return None, f"http:{exc}"

    try:
        root = ET.fromstring(r.content)
    except ET.ParseError as exc:
        return None, f"parse:{exc}"

    site_el = root.find("./SITE")
    if site_el is None:
        return None, "missing_site"

    lat_txt = _text(site_el, "./LATITUDE")
    lon_txt = _text(site_el, "./LONGITUDE")
    country = _text(site_el, "./COUNTRY") or _text(site_el, "./COUNTRY_CODE")
    roc = _text(site_el, "./ROC")
    pue_val = _extract_pue_from_extensions(site_el)

    out = {
        "latitude": float(lat_txt) if lat_txt else None,
        "longitude": float(lon_txt) if lon_txt else None,
        "country": country,
        "roc": roc,
        "pue": pue_val,
    }
    return out, None


# --- Country helpers ------------------------------------------------------

# Minimal name→ISO2 map covering GOCDB country labels seen in sites_raw.json
_COUNTRY_NAME_TO_CODE = {
    "algeria": "DZ",
    "argentina": "AR",
    "armenia": "AM",
    "australia": "AU",
    "austria": "AT",
    "azerbaijan": "AZ",
    "belarus": "BY",
    "belgium": "BE",
    "brazil": "BR",
    "bulgaria": "BG",
    "canada": "CA",
    "chile": "CL",
    "china": "CN",
    "colombia": "CO",
    "croatia": "HR",
    "czech republic": "CZ",
    "denmark": "DK",
    "egypt": "EG",
    "estonia": "EE",
    # FYROM is the historic GOCDB label for North Macedonia
    "fyrom": "MK",
    "finland": "FI",
    "france": "FR",
    "georgia": "GE",
    "germany": "DE",
    "greece": "GR",
    "hungary": "HU",
    "india": "IN",
    "iran": "IR",
    "ireland": "IE",
    "israel": "IL",
    "italy": "IT",
    "japan": "JP",
    "latvia": "LV",
    "lithuania": "LT",
    "malaysia": "MY",
    "mexico": "MX",
    "moldova": "MD",
    "montenegro": "ME",
    "morocco": "MA",
    "netherlands": "NL",
    "new zealand": "NZ",
    "norway": "NO",
    "pakistan": "PK",
    "philippines": "PH",
    "poland": "PL",
    "portugal": "PT",
    "puerto rico": "PR",
    "romania": "RO",
    "russia": "RU",
    "senegal": "SN",
    "serbia": "RS",
    "slovakia": "SK",
    "slovenia": "SI",
    "south africa": "ZA",
    "south korea": "KR",
    "spain": "ES",
    "sweden": "SE",
    "switzerland": "CH",
    "taiwan": "TW",
    "thailand": "TH",
    "turkey": "TR",
    "ukraine": "UA",
    "united kingdom": "GB",
    "united states of america": "US",
    "venezuela": "VE",
    "vietnam": "VN",
}


def _country_code_from_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    return _COUNTRY_NAME_TO_CODE.get(name.strip().lower())


def _load_country_centroids() -> Dict[str, Tuple[float, float]]:
    if not COUNTRY_LATLNG_PATH.exists():
        return {}
    try:
        raw = json.loads(COUNTRY_LATLNG_PATH.read_text(encoding="utf-8"))
        return {
            entry["country_code"].upper(): (float(entry["lat"]), float(entry["lng"]))
            for entry in raw
            if entry.get("country_code") is not None
        }
    except Exception:
        return {}


def main(infile, outfile):
    # Load fallback coordinates
    with open(SITE_COORDS_PATH, "r", encoding="utf-8") as f:
        site_coords = json.load(f)

    country_centroids = _load_country_centroids()

    # Load raw site list (output/sites_raw.json)
    with open(infile, "r", encoding="utf-8") as f:
        data = json.load(f)

    session = _new_session()

    for site in data:
        name = site.get("site_name")
        # Defaults: site-specific fallback (when meaningful), then country centroid, else neutral default
        fallback_lat, fallback_lon = (None, None)
        if name in site_coords:
            latlon = site_coords.get(name)
            if isinstance(latlon, (list, tuple)) and len(latlon) == 2:
                # Treat (0,0) placeholders as missing
                if not (latlon[0] == 0.0 and latlon[1] == 0.0):
                    fallback_lat, fallback_lon = latlon

        country_code = site.get("country_code") or _country_code_from_name(site.get("country"))
        if (fallback_lat is None or fallback_lon is None) and country_code:
            centroid = country_centroids.get(country_code.upper())
            if centroid:
                fallback_lat, fallback_lon = centroid

        if fallback_lat is None or fallback_lon is None:
            fallback_lat, fallback_lon = (52.0, 5.0)

        chosen_lat, chosen_lon = fallback_lat, fallback_lon
        chosen_pue = STATIC_PUE

        # Try GOCDB per site
        if name:
            goc_data, err = fetch_site_from_gocdb(session, name)
            if goc_data:
                if goc_data.get("latitude") is not None or goc_data.get("longitude") is not None:
                    chosen_lat = goc_data.get("latitude", chosen_lat)
                    chosen_lon = goc_data.get("longitude", chosen_lon)
                if goc_data.get("pue") is not None:
                    chosen_pue = goc_data["pue"]
                # Prefer fresher GOCDB country/roc when present
                if goc_data.get("country"):
                    site["country"] = goc_data["country"]
                if goc_data.get("roc"):
                    site["roc"] = goc_data["roc"]
            else:
                # Note the failure reason while keeping fallback coords
                if err:
                    site["error"] = err

        # Ensure country_code present for downstream consumers
        site["country_code"] = site.get("country_code") or _country_code_from_name(site.get("country"))

        site["latitude"] = chosen_lat
        site["longitude"] = chosen_lon
        site["pue"] = chosen_pue

    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python gocdb_postprocess.py input.json output.json")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
