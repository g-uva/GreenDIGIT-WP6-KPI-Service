#!/usr/bin/env python
"""
Build a mapping: site_name -> latitude/longitude from GOCDB.

It first fetches the site list (public), then for each site makes a private
`get_site&sitename=...` call to grab LATITUDE/LONGITUDE (if authorized).

Auth options:
  1) OAuth2 Bearer token (EGI Check-in):  --token $TOKEN  (or env GOCDB_OAUTH_TOKEN)
  2) X.509 client cert:                  --cert client.pem [--key client.key]

Example:
  python gocdb_site_latlng.py --format csv --output site_latlng.csv \
      --scope EGI --max-workers 6 \
      --token "$GOCDB_TOKEN"

Docs:
  - get_site_list (public) and get_site (private; includes LATITUDE/LONGITUDE)
"""

from __future__ import annotations
import argparse
import csv
import json
import sys
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode

import requests
import xml.etree.ElementTree as ET

DEFAULT_BASE_URL = "https://goc.egi.eu/gocdbpi"
PUBLIC_ENTRY = "/public/"
PRIVATE_ENTRY = "/private/"

# Cert/key configuration (optional, mirrors kpi_service/app/main.py behavior)
_default_cert_base = os.getenv("GOCDB_CERT_BASE")
if not _default_cert_base:
    _default_cert_base = ".cert" if os.path.isdir(".cert") else "/etc/gocdb-cert"

_default_cert_path = os.path.join(_default_cert_base, "GDIGIT_Cert.pem")
_default_key_path = os.path.join(_default_cert_base, "gd_gocdb_private.pem")

GOCDB_CERT = os.getenv("GOCDB_CERT") if os.getenv("GOCDB_CERT") else (_default_cert_path if os.path.exists(_default_cert_path) else None)
GOCDB_KEY = os.getenv("GOCDB_KEY") if os.getenv("GOCDB_KEY") else (_default_key_path if os.path.exists(_default_key_path) else None)
GOCDB_CA = os.getenv("GOCDB_CA")

NS = {}  # no namespaces in the examples, keep simple

def build_url(base_url: str, entry: str, method: str, **params) -> str:
    q = {"method": method}
    q.update({k: v for k, v in params.items() if v is not None})
    return f"{base_url}{entry}?{urlencode(q)}"

def new_session(token: Optional[str], cert: Optional[str], key: Optional[str], ca: Optional[str], timeout: float) -> requests.Session:
    s = requests.Session()
    s.headers["Accept"] = "application/xml"
    if token:
        s.headers["Authorization"] = f"Bearer {token}"
    if cert:
        s.cert = (cert, key) if key else cert
    if ca:
        s.verify = ca
    s.timeout = timeout
    return s

def fetch_site_list(session: requests.Session, base_url: str, entry: str, scope: Optional[str], roc: Optional[str], country: Optional[str]) -> List[Dict]:
    url = build_url(base_url, entry, "get_site_list", scope=scope, roc=roc, country=country)
    r = session.get(url)
    r.raise_for_status()
    root = ET.fromstring(r.content)
    sites = []
    for site in root.findall("./SITE", NS):
        sites.append({
            "name": site.get("NAME") or "",
            "roc": site.get("ROC"),
            "country": site.get("COUNTRY_CODE"),
            "id": site.get("ID"),
            "certification_status": site.get("CERTIFICATION_STATUS"),
            "production_infrastructure": site.get("PRODUCTION_INFRASTRUCTURE"),
        })
    return sites

def fetch_site_latlng(session: requests.Session, base_url: str, entry: str, sitename: str, scope: Optional[str]) -> Tuple[Optional[float], Optional[float], Optional[str], Optional[str], Optional[str]]:
    """Returns (lat, lng, country, country_code, error) for a single site name."""
    url = build_url(base_url, entry, "get_site", sitename=sitename, scope=scope)
    try:
        r = session.get(url)
        if r.status_code in (401, 403):
            return None, None, None, None, f"unauthorized:{r.status_code}"
        r.raise_for_status()
        root = ET.fromstring(r.content)
        site = root.find("./SITE", NS)
        if site is None:
            return None, None, None, None, "not_found"
        lat_el = site.find("./LATITUDE", NS)
        lng_el = site.find("./LONGITUDE", NS)
        country_el = site.find("./COUNTRY", NS)
        country_code_el = site.find("./COUNTRY_CODE", NS)
        lat = float(lat_el.text) if lat_el is not None and lat_el.text else None
        lng = float(lng_el.text) if lng_el is not None and lng_el.text else None
        country = country_el.text.strip() if country_el is not None and country_el.text else None
        country_code = country_code_el.text.strip() if country_code_el is not None and country_code_el.text else None
        # Some responses store country/country_code as attributes instead of elements
        country = country or site.attrib.get("COUNTRY")
        country_code = country_code or site.attrib.get("COUNTRY_CODE")
        return lat, lng, country, country_code, None
    except Exception as e:
        return None, None, None, None, f"error:{type(e).__name__}"

def main():
    ap = argparse.ArgumentParser(description="Build site → (lat,lng) mapping from GOCDB.")
    ap.add_argument("--base-url", default=DEFAULT_BASE_URL, help=f"Base API URL (default: {DEFAULT_BASE_URL})")
    ap.add_argument("--scope", default=None, help="Filter by scope (e.g., EGI). Applied to both calls.")
    ap.add_argument("--roc", default=None, help="Filter by ROC/NGI for get_site_list.")
    ap.add_argument("--country", default=None, help="Filter by country for get_site_list.")
    ap.add_argument("--token", default=None, help="OAuth2 Bearer token (EGI Check-in). Defaults to env GOCDB_OAUTH_TOKEN.")
    ap.add_argument("--cert", default=None, help="Path to client certificate PEM (for mTLS). Optional; defaults to env GOCDB_CERT when set.")
    ap.add_argument("--key", default=None, help="Path to client key PEM (if separate). Optional; defaults to env GOCDB_KEY when set.")
    ap.add_argument("--ca", default=None, help="CA bundle to verify the GOCDB endpoint. Optional; defaults to env GOCDB_CA when set.")
    ap.add_argument("--format", choices=["csv", "json"], default="csv", help="Output format.")
    ap.add_argument("--output", default="-", help="Output file path or '-' for stdout.")
    ap.add_argument("--max-workers", type=int, default=5, help="Parallel workers for per-site fetches.")
    ap.add_argument("--timeout", type=float, default=30.0, help="Request timeout in seconds.")
    ap.add_argument("--sleep", type=float, default=0.0, help="Optional fixed delay between requests (politeness).")
    args = ap.parse_args()

    token = args.token or os.environ.get("GOCDB_OAUTH_TOKEN")

    # Cert/key/CA are optional; only enable mTLS when paths are provided/resolve.
    cert = args.cert or (GOCDB_CERT.strip() if GOCDB_CERT else None)
    key = args.key or (GOCDB_KEY.strip() if GOCDB_KEY else None)
    ca = args.ca or (GOCDB_CA.strip() if GOCDB_CA else None)

    entry = PRIVATE_ENTRY if (token or cert or key) else PUBLIC_ENTRY
    session = new_session(token, cert, key, ca, args.timeout)

    # 1) site list (public)
    sites = fetch_site_list(session, args.base_url, entry, args.scope, args.roc, args.country)
    if not sites:
        print("No sites returned by get_site_list (check filters).", file=sys.stderr)
        return 2

    # Log the full site list to stderr (keeps stdout clean for CSV/JSON output)
    site_entries = sorted(
        [(s.get("name", ""), s.get("country")) for s in sites if s.get("name")]
    )
    sys.stderr.write(f"[gocdb] get_site_list returned {len(site_entries)} sites:\n")
    for name, country in site_entries:
        country_disp = country or "unknown"
        sys.stderr.write(f" - {name} ({country_disp})\n")
    sys.stderr.flush()

    # 2) fetch lat/lng per site (private – may return unauthorized)
    results: List[Dict] = []
    def task(site):
        if args.sleep:
            time.sleep(args.sleep)
        lat, lng, ctry_detail, ctry_code, err = fetch_site_latlng(session, args.base_url, entry, site["name"], args.scope)
        country = ctry_detail or site["country"]
        out = {
            "site_name": site["name"],
            "roc": site["roc"],
            "country": country,
            "country_code": ctry_code,
            "certification_status": site["certification_status"],
            "production_infrastructure": site["production_infrastructure"],
            "latitude": lat,
            "longitude": lng,
            "error": err,
        }
        return out

    if args.max_workers <= 1:
        for s in sites:
            results.append(task(s))
    else:
        with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
            futs = [ex.submit(task, s) for s in sites]
            for f in as_completed(futs):
                results.append(f.result())

    # Sort for stable output
    results.sort(key=lambda d: d["site_name"].lower())

    # Basic stats to stderr
    have = sum(1 for r in results if r["latitude"] is not None and r["longitude"] is not None)
    unauth = sum(1 for r in results if r["error"] and r["error"].startswith("unauthorized"))
    sys.stderr.write(f"Sites: {len(results)} | with coords: {have} | unauthorized: {unauth}\n")
    
    # Ensure that the folder exists
    try:
        os.makedirs(os.path.dirname(args.output), exist_ok=True)
    except Exception as e:
        return None, None, f"error:{type(e).__name__}"

    # 3) write output
    if args.output == "-":
        outfh = sys.stdout
        close = False
    else:
        outfh = open(args.output, "w", newline="", encoding="utf-8")
        close = True

    try:
        if args.format == "json":
            json.dump(results, outfh, indent=2, ensure_ascii=False)
            if outfh is sys.stdout:
                print()
        else:
            fieldnames = ["site_name","roc","country","country_code","certification_status","production_infrastructure","latitude","longitude","error"]
            w = csv.DictWriter(outfh, fieldnames=fieldnames)
            w.writeheader()
            for row in results:
                w.writerow(row)
    finally:
        if close:
            outfh.close()

if __name__ == "__main__":
    import os
    sys.exit(main())
