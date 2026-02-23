from __future__ import annotations

import json
import importlib.util
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Optional, Sequence, Tuple


class BiddingZoneResolverError(RuntimeError):
    pass


class BiddingZoneNotFoundError(BiddingZoneResolverError):
    pass


@dataclass(frozen=True)
class PolygonData:
    # First ring is outer shell; following rings are holes.
    rings: List[List[Tuple[float, float]]]


@dataclass(frozen=True)
class BiddingZoneFeature:
    zone_name: str
    polygons: List[PolygonData]
    bbox: Tuple[float, float, float, float]  # min_lon, min_lat, max_lon, max_lat
    area: float


def _ring_area(ring: Sequence[Tuple[float, float]]) -> float:
    if len(ring) < 3:
        return 0.0
    s = 0.0
    for i in range(len(ring) - 1):
        x1, y1 = ring[i]
        x2, y2 = ring[i + 1]
        s += x1 * y2 - x2 * y1
    return abs(s) / 2.0


def _point_on_segment(
    px: float,
    py: float,
    x1: float,
    y1: float,
    x2: float,
    y2: float,
    eps: float = 1e-12,
) -> bool:
    # Collinear and within segment bounds.
    cross = abs((px - x1) * (y2 - y1) - (py - y1) * (x2 - x1))
    if cross > eps:
        return False
    if min(x1, x2) - eps <= px <= max(x1, x2) + eps and min(y1, y2) - eps <= py <= max(y1, y2) + eps:
        return True
    return False


def _point_in_ring(px: float, py: float, ring: Sequence[Tuple[float, float]]) -> bool:
    # Ray casting; boundary is treated as inside.
    inside = False
    n = len(ring)
    if n < 4:
        return False

    for i in range(n - 1):
        x1, y1 = ring[i]
        x2, y2 = ring[i + 1]

        if _point_on_segment(px, py, x1, y1, x2, y2):
            return True

        cond = (y1 > py) != (y2 > py)
        if cond:
            xinters = (x2 - x1) * (py - y1) / (y2 - y1) + x1
            if px < xinters:
                inside = not inside

    return inside


def _point_in_polygon(px: float, py: float, polygon: PolygonData) -> bool:
    if not polygon.rings:
        return False
    outer = polygon.rings[0]
    if not _point_in_ring(px, py, outer):
        return False
    for hole in polygon.rings[1:]:
        if _point_in_ring(px, py, hole):
            return False
    return True


class BiddingZoneResolver:
    """Load local ENTSO-E zone GeoJSON files and resolve lat/lon to zone + EIC."""

    def __init__(
        self,
        geojson_dir: Path,
        *,
        use_spatial_index: bool = True,
        area_lookup: Optional[Callable[[str], object]] = None,
    ) -> None:
        self.geojson_dir = geojson_dir
        self.use_spatial_index = use_spatial_index
        self._features: List[BiddingZoneFeature] = []
        self._area_lookup = area_lookup or self._default_area_lookup()
        self._load_geojsons()

    @staticmethod
    def _default_area_lookup() -> Callable[[str], object]:
        # 1) Preferred: entsoe-py package
        try:
            from entsoe.mappings import lookup_area
            return lookup_area
        except Exception:
            pass

        # 2) Common typo'd vendor package name
        try:
            from entsoe.mappings import lookup_area
            return lookup_area
        except Exception:
            pass

        # 3) Load vendored mappings.py from explicit path / known defaults
        candidates = []
        env_path = os.getenv("BZ_MAPPINGS_PY")
        if env_path:
            candidates.append(Path(env_path))
        here = Path(__file__).resolve().parent
        candidates.extend(
            [
                (here / ".." / "entsoe" / "mappings.py").resolve(),
                (here / ".." / "entsoe" / "mappings.py").resolve(),
            ]
        )
        for p in candidates:
            if not p.is_file():
                continue
            spec = importlib.util.spec_from_file_location("entsoe_mappings_vendored", p)
            if spec is None or spec.loader is None:
                continue
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            lookup_area = getattr(module, "lookup_area", None)
            if callable(lookup_area):
                return lookup_area

        raise BiddingZoneResolverError(
            "Could not import lookup_area. "
            "Install entsoe-py, or provide a vendored mappings.py at "
            "entsoe/mappings.py (or entsoe/mappings.py), or set BZ_MAPPINGS_PY."
        )

    @staticmethod
    def _parse_polygon_coords(raw: object) -> PolygonData:
        if not isinstance(raw, list):
            raise BiddingZoneResolverError("Polygon coordinates must be a list")
        rings: List[List[Tuple[float, float]]] = []
        for ring in raw:
            if not isinstance(ring, list) or len(ring) < 4:
                continue
            parsed_ring: List[Tuple[float, float]] = []
            for p in ring:
                if not isinstance(p, list) or len(p) < 2:
                    continue
                parsed_ring.append((float(p[0]), float(p[1])))
            if len(parsed_ring) >= 4:
                if parsed_ring[0] != parsed_ring[-1]:
                    parsed_ring.append(parsed_ring[0])
                rings.append(parsed_ring)
        if not rings:
            raise BiddingZoneResolverError("Polygon has no valid rings")
        return PolygonData(rings=rings)

    @classmethod
    def _parse_geometry(cls, geom: dict) -> List[PolygonData]:
        gtype = geom.get("type")
        coords = geom.get("coordinates")
        if gtype == "Polygon":
            return [cls._parse_polygon_coords(coords)]
        if gtype == "MultiPolygon":
            if not isinstance(coords, list):
                raise BiddingZoneResolverError("MultiPolygon coordinates must be a list")
            return [cls._parse_polygon_coords(poly) for poly in coords]
        raise BiddingZoneResolverError(f"Unsupported geometry type: {gtype}")

    @staticmethod
    def _bbox_for_polygons(polygons: Sequence[PolygonData]) -> Tuple[float, float, float, float]:
        lons: List[float] = []
        lats: List[float] = []
        for poly in polygons:
            for ring in poly.rings:
                for lon, lat in ring:
                    lons.append(lon)
                    lats.append(lat)
        if not lons or not lats:
            raise BiddingZoneResolverError("Invalid polygon data for bbox")
        return min(lons), min(lats), max(lons), max(lats)

    @staticmethod
    def _area_for_polygons(polygons: Sequence[PolygonData]) -> float:
        area = 0.0
        for poly in polygons:
            if not poly.rings:
                continue
            shell = _ring_area(poly.rings[0])
            holes = sum(_ring_area(r) for r in poly.rings[1:])
            area += max(0.0, shell - holes)
        return area

    def _load_geojsons(self) -> None:
        if not self.geojson_dir.exists() or not self.geojson_dir.is_dir():
            raise BiddingZoneResolverError(f"GeoJSON directory not found: {self.geojson_dir}")

        paths = sorted(self.geojson_dir.glob("*.geojson"))
        if not paths:
            raise BiddingZoneResolverError(f"No GeoJSON files found in: {self.geojson_dir}")

        features: List[BiddingZoneFeature] = []
        for path in paths:
            with path.open("r", encoding="utf-8") as f:
                doc = json.load(f)
            if not isinstance(doc, dict) or doc.get("type") != "FeatureCollection":
                continue
            raw_features = doc.get("features") or []
            for idx, raw in enumerate(raw_features):
                props = raw.get("properties") or {}
                zone_name = str(props.get("zoneName") or "").strip()
                if not zone_name:
                    raise BiddingZoneResolverError(
                        f"{path.name} feature #{idx} missing properties.zoneName"
                    )

                geom = raw.get("geometry")
                if not isinstance(geom, dict):
                    continue
                polygons = self._parse_geometry(geom)
                bbox = self._bbox_for_polygons(polygons)
                area = self._area_for_polygons(polygons)
                features.append(
                    BiddingZoneFeature(
                        zone_name=zone_name,
                        polygons=polygons,
                        bbox=bbox,
                        area=area,
                    )
                )

        if not features:
            raise BiddingZoneResolverError("No valid bidding zone features loaded")

        self._features = features

    @staticmethod
    def _validate_lat_lon(lat: float, lon: float) -> None:
        if not (-90.0 <= lat <= 90.0):
            raise ValueError(f"latitude out of range: {lat}")
        if not (-180.0 <= lon <= 180.0):
            raise ValueError(f"longitude out of range: {lon}")

    @staticmethod
    def _bbox_contains(bbox: Tuple[float, float, float, float], lon: float, lat: float) -> bool:
        min_lon, min_lat, max_lon, max_lat = bbox
        return min_lon <= lon <= max_lon and min_lat <= lat <= max_lat

    def resolve_zone_name(self, lat: float, lon: float) -> str:
        self._validate_lat_lon(lat, lon)

        matches: List[BiddingZoneFeature] = []
        for feature in self._features:
            if self.use_spatial_index and not self._bbox_contains(feature.bbox, lon, lat):
                continue
            for polygon in feature.polygons:
                if _point_in_polygon(lon, lat, polygon):
                    matches.append(feature)
                    break

        if not matches:
            raise BiddingZoneNotFoundError(
                f"COORDS_OUTSIDE_SUPPORTED_ZONES lat={lat} lon={lon}"
            )

        if len(matches) == 1:
            return matches[0].zone_name

        # Rare overlaps: pick smallest polygon area.
        smallest = min(matches, key=lambda f: f.area)
        return smallest.zone_name

    def resolve(self, lat: float, lon: float) -> Tuple[str, str]:
        zone_name = self.resolve_zone_name(lat, lon)
        area_obj = self._area_lookup(zone_name)
        bz_eic = str(getattr(area_obj, "value", "")).strip()
        if not bz_eic:
            raise BiddingZoneResolverError(f"No EIC mapping found for zoneName={zone_name}")
        return zone_name, bz_eic
