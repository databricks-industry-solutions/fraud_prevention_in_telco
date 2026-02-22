#!/usr/bin/env python3
"""
Load US state boundaries (GeoJSON) and sample random points inside state polygons.
Used for location generation that respects actual land borders (e.g. Alaska, Hawaii).
"""
from __future__ import annotations

import json
import random
from typing import Any

try:
    from shapely.geometry import shape, Point
except ImportError:
    shape = Point = None  # type: ignore[misc, assignment]

# Census 20m simplified states (STUSPS = state abbreviation)
US_STATES_GEOJSON_URL = (
    "https://raw.githubusercontent.com/jkiefn1/SO_Json_Question/main/States_20m.geojson"
)


def _load_geojson_from_url(url: str) -> dict:
    import urllib.request
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.load(resp)


def load_state_polygons(
    url: str | None = None,
    local_path: str | None = None,
) -> dict[str, Any]:
    """
    Load US state boundaries and return a dict mapping state abbreviation (STUSPS) to Shapely geometry.
    Geometry can be Polygon or MultiPolygon. Coordinates are in (lon, lat) / (x, y).
    """
    if shape is None or Point is None:
        raise ImportError("shapely is required for polygon-based location sampling. Install with: pip install shapely")
    if local_path:
        with open(local_path) as f:
            fc = json.load(f)
    else:
        fc = _load_geojson_from_url(url or US_STATES_GEOJSON_URL)
    out: dict[str, Any] = {}
    for feat in fc.get("features", []):
        props = feat.get("properties") or {}
        stusps = props.get("STUSPS")
        if not stusps:
            continue
        geom = feat.get("geometry")
        if not geom:
            continue
        try:
            shp = shape(geom)
            if shp.is_empty or not shp.is_valid:
                continue
            out[str(stusps).strip()] = shp
        except Exception:
            continue
    return out


def random_point_in_geometry(
    geom: Any,
    rng: random.Random | None = None,
    max_attempts: int = 500,
) -> tuple[float, float]:
    """
    Return a random (lat, lon) point inside the geometry using rejection sampling.
    Works with Polygon and MultiPolygon. GeoJSON/Shapely use (lon, lat) so we return (lat, lon).
    """
    if rng is None:
        rng = random
    minx, miny, maxx, maxy = geom.bounds
    for _ in range(max_attempts):
        x = rng.uniform(minx, maxx)
        y = rng.uniform(miny, maxy)
        p = Point(x, y)
        if geom.contains(p):
            return (float(y), float(x))  # lat, lon
    # Fallback: centroid (guaranteed inside for convex; for concave might be outside but rare)
    c = geom.centroid
    return (float(c.y), float(c.x))


# Module-level cache for state polygons (avoids re-downloading)
_STATE_POLYGONS_CACHE: dict[str, Any] = {}


def get_state_polygons_cached(
    url: str | None = None,
    local_path: str | None = None,
) -> dict[str, Any]:
    """Load state polygons once and cache at module level."""
    if _STATE_POLYGONS_CACHE:
        return _STATE_POLYGONS_CACHE
    polygons = load_state_polygons(url=url, local_path=local_path)
    _STATE_POLYGONS_CACHE.update(polygons)
    return _STATE_POLYGONS_CACHE
