from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bidding_zone_resolver import BiddingZoneNotFoundError, BiddingZoneResolver


def _write_zone(path: Path, zone_name: str, coords: list[list[float]]) -> None:
    doc = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"zoneName": zone_name},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [coords],
                },
            }
        ],
    }
    path.write_text(json.dumps(doc), encoding="utf-8")


class BiddingZoneResolverTests(unittest.TestCase):
    def test_resolve_golden_coordinates(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            _write_zone(
                base / "DK_1.geojson",
                "DK_1",
                [[8.0, 55.0], [10.0, 55.0], [10.0, 57.0], [8.0, 57.0], [8.0, 55.0]],
            )
            _write_zone(
                base / "DE_LU.geojson",
                "DE_LU",
                [[10.0, 55.0], [12.0, 55.0], [12.0, 57.0], [10.0, 57.0], [10.0, 55.0]],
            )

            eic_map = {"DK_1": "10YDK-1--------W", "DE_LU": "10Y1001A1001A82H"}
            resolver = BiddingZoneResolver(
                base,
                area_lookup=lambda z: type("Area", (), {"value": eic_map[z]})(),
            )

            zone, eic = resolver.resolve(56.0, 9.0)
            self.assertEqual(zone, "DK_1")
            self.assertEqual(eic, "10YDK-1--------W")

            zone, eic = resolver.resolve(56.0, 11.0)
            self.assertEqual(zone, "DE_LU")
            self.assertEqual(eic, "10Y1001A1001A82H")

    def test_outside_returns_not_found(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            _write_zone(
                base / "FR.geojson",
                "FR",
                [[2.0, 45.0], [3.0, 45.0], [3.0, 46.0], [2.0, 46.0], [2.0, 45.0]],
            )
            resolver = BiddingZoneResolver(base, area_lookup=lambda z: type("Area", (), {"value": z})())
            with self.assertRaises(BiddingZoneNotFoundError):
                resolver.resolve(50.0, 10.0)

    def test_overlap_picks_smallest_area(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            _write_zone(
                base / "BIG.geojson",
                "BIG",
                [[0.0, 0.0], [10.0, 0.0], [10.0, 10.0], [0.0, 10.0], [0.0, 0.0]],
            )
            _write_zone(
                base / "SMALL.geojson",
                "SMALL",
                [[4.0, 4.0], [6.0, 4.0], [6.0, 6.0], [4.0, 6.0], [4.0, 4.0]],
            )

            resolver = BiddingZoneResolver(
                base,
                area_lookup=lambda z: type("Area", (), {"value": f"EIC-{z}"})(),
            )

            zone, eic = resolver.resolve(5.0, 5.0)
            self.assertEqual(zone, "SMALL")
            self.assertEqual(eic, "EIC-SMALL")


if __name__ == "__main__":
    unittest.main()
