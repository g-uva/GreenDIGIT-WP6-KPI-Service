#!/usr/bin/env python3
"""Generate a country → {lat, lng} mapping from country codes.

Defaults:
  - Input:  gocdb_fetch_service/countries.txt (one code per line, bullets allowed)
  - Output: output/country_latlng.json
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Optional

from countryinfo import CountryInfo

# Manual fallbacks for codes missing in CountryInfo.latlng()
MANUAL_COORDS = {
    "RS": (44.0165, 21.0059),  # Serbia
    "ME": (42.7087, 19.3744),  # Montenegro
}


def _clean_code(raw: str) -> Optional[str]:
    code = raw.strip()
    if not code:
        return None
    if code.startswith("-"):
        code = code.lstrip("-").strip()
    return code.upper() or None


def parse_codes(path: Path) -> List[str]:
    seen = set()
    codes: List[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        code = _clean_code(line)
        if not code or code in seen:
            continue
        seen.add(code)
        codes.append(code)
    return codes


def lookup_latlng(code: str) -> Optional[tuple]:
    # Manual overrides first
    if code in MANUAL_COORDS:
        return MANUAL_COORDS[code]
    try:
        ci = CountryInfo(code)
        latlng = ci.latlng()
        if latlng and len(latlng) >= 2:
            return float(latlng[0]), float(latlng[1])
    except Exception as exc:  # noqa: BLE001
        print(f"[warn] Failed to resolve {code}: {exc}", file=sys.stderr)
    return None


def build_mapping(codes: List[str]) -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []
    for code in codes:
        coords = lookup_latlng(code)
        lat, lng = (coords if coords else (None, None))
        out.append({"country_code": code, "lat": lat, "lng": lng})
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        default="gocdb_fetch_service/countries.txt",
        help="Path to input country codes list",
    )
    parser.add_argument(
        "--output",
        default="output/country_latlng.json",
        help="Path to output JSON mapping",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        print(f"[error] Input file not found: {input_path}", file=sys.stderr)
        return 1

    codes = parse_codes(input_path)
    mapping = build_mapping(codes)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(mapping, indent=2), encoding="utf-8")

    missing = [m["country_code"] for m in mapping if m["lat"] is None or m["lng"] is None]
    print(f"[ok] Wrote {len(mapping)} entries to {output_path}")
    if missing:
        print(f"[warn] Missing coordinates for: {', '.join(missing)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
