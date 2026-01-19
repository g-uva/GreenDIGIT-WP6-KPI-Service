#!/usr/bin/env python3
"""
gocdb_postprocess.py
Adds mock lat/lng and static PUE to GOC DB site JSON.
"""

import json
import sys
from pathlib import Path

STATIC_PUE = 1.7
SITE_COORDS_PATH = Path(__file__).resolve().parent.parent / "site_coords.json"

def main(infile, outfile):
    with open(SITE_COORDS_PATH, "r", encoding="utf-8") as f:
        site_coords = json.load(f)

    with open(infile, "r", encoding="utf-8") as f:
        data = json.load(f)

    for site in data:
        name = site.get("site_name")
        lat, lon = site_coords.get(name, (52.0, 5.0))  # fallback (NL)
        site["latitude"] = lat
        site["longitude"] = lon
        site["pue"] = STATIC_PUE

    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python gocdb_postprocess.py input.json output.json")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
