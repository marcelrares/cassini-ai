from __future__ import annotations

import json
import math
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .config import MonitorConfig
from .geo import bbox


RESOURCE_DIR = Path("data/resources")
MAX_OVERPASS_BBOX_DEGREES2 = 25.0


@dataclass(frozen=True)
class ResourceSummary:
    boundary_source: str
    osm_source: str
    waterway_features: int
    water_body_features: int
    urban_landuse_features: int
    forest_or_green_features: int
    agriculture_features: int
    land_cover_percentages: dict[str, float]
    land_cover_hectares: dict[str, float]
    land_cover_basis: str
    cached_files: list[str]
    explanation: str


def ensure_resources(config: MonitorConfig, *, update: bool = False) -> ResourceSummary:
    boundary_path, osm_context_path = _resource_paths(config)
    boundary_path.parent.mkdir(parents=True, exist_ok=True)
    if update or not boundary_path.exists():
        _write_config_boundary(config, boundary_path)
    if update or not osm_context_path.exists():
        _fetch_osm_context(config, osm_context_path)
    return load_resource_summary(config)


def load_resource_summary(config: MonitorConfig) -> ResourceSummary:
    boundary_path, osm_context_path = _resource_paths(config)
    osm = _read_json(osm_context_path, default={"elements": []})
    elements = osm.get("elements", []) if isinstance(osm, dict) else []
    counts = _feature_counts(elements)
    land_cover = _land_cover_stats(elements)
    osm_status = str(osm.get("status", "ready")) if isinstance(osm, dict) else "ready"
    return ResourceSummary(
        boundary_source="Configured area boundary" if boundary_path.exists() else "not available",
        osm_source=_osm_source_label(osm_status, osm_context_path),
        waterway_features=counts["waterway"],
        water_body_features=counts["water_body"],
        urban_landuse_features=counts["urban"],
        forest_or_green_features=counts["green"],
        agriculture_features=counts["agriculture"],
        land_cover_percentages=land_cover["percentages"],
        land_cover_hectares=land_cover["hectares"],
        land_cover_basis=str(land_cover["basis"]),
        cached_files=[str(path) for path in [boundary_path, osm_context_path] if path.exists()],
        explanation=(
            "Local resource cache adds hydrology, water bodies, urban/impervious proxies, vegetation/forest, and agriculture context. "
            "These features refine relative exposure; they are not official hazard maps."
        ),
    )


def resource_summary_to_dict(summary: ResourceSummary) -> dict[str, object]:
    return asdict(summary)


def load_osm_context(config: MonitorConfig) -> dict[str, object] | None:
    _, osm_context_path = _resource_paths(config)
    if not osm_context_path.exists():
        return None
    return _read_json(osm_context_path, default=None)


def _resource_paths(config: MonitorConfig) -> tuple[Path, Path]:
    slug = _slug(f"{config.area_name}-{_bbox_slug(config)}")
    area_dir = RESOURCE_DIR / slug
    return area_dir / "boundary.geojson", area_dir / "osm_context.json"


def _write_config_boundary(config: MonitorConfig, path: Path) -> None:
    payload = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "id": _slug(config.area_name),
                "geometry": {"type": "Polygon", "coordinates": [config.polygon]},
                "properties": {"name": config.area_name, "source": "runtime_bbox_or_config_polygon"},
            }
        ],
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _fetch_osm_context(config: MonitorConfig, path: Path) -> None:
    west, south, east, north = bbox(config.polygon)
    if _bbox_degrees2(west, south, east, north) > MAX_OVERPASS_BBOX_DEGREES2:
        payload = {
            "status": "skipped_large_area",
            "reason": "bbox too large for one Overpass request; split into tiles for detailed OSM context",
            "bbox": [west, south, east, north],
            "elements": [],
        }
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return
    query = f"""
    [out:json][timeout:60];
    (
      way["waterway"]({south},{west},{north},{east});
      relation["waterway"]({south},{west},{north},{east});
      way["natural"="water"]({south},{west},{north},{east});
      relation["natural"="water"]({south},{west},{north},{east});
      way["landuse"~"^(reservoir|basin|residential|industrial|commercial|retail|construction|forest|meadow|grass|farmland|orchard|vineyard)$"]({south},{west},{north},{east});
      relation["landuse"~"^(reservoir|basin|residential|industrial|commercial|retail|construction|forest|meadow|grass|farmland|orchard|vineyard)$"]({south},{west},{north},{east});
      way["natural"~"^(wood|scrub|grassland|wetland)$"]({south},{west},{north},{east});
      relation["natural"~"^(wood|scrub|grassland|wetland)$"]({south},{west},{north},{east});
    );
    out geom tags;
    """
    request = Request(
        "https://overpass-api.de/api/interpreter",
        data=urlencode({"data": query}).encode("utf-8"),
        headers={"User-Agent": "calamity-ai-monitor/1.0"},
        method="POST",
    )
    with urlopen(request, timeout=120) as response:
        payload = json.loads(response.read().decode("utf-8"))
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _osm_source_label(status: str, path: Path) -> str:
    if not path.exists():
        return "not available"
    if status == "skipped_large_area":
        return "skipped: area too large for one Overpass request"
    return "OpenStreetMap/Overpass cached extract"


def _feature_counts(elements: list[dict[str, object]]) -> dict[str, int]:
    counts = {"waterway": 0, "water_body": 0, "urban": 0, "green": 0, "agriculture": 0}
    for element in elements:
        tags = element.get("tags", {})
        if not isinstance(tags, dict):
            continue
        if "waterway" in tags:
            counts["waterway"] += 1
        if tags.get("natural") == "water" or tags.get("landuse") in {"reservoir", "basin"}:
            counts["water_body"] += 1
        if tags.get("landuse") in {"residential", "industrial", "commercial", "retail", "construction"}:
            counts["urban"] += 1
        if tags.get("landuse") in {"forest", "meadow", "grass"} or tags.get("natural") in {"wood", "scrub", "grassland", "wetland"}:
            counts["green"] += 1
        if tags.get("landuse") in {"farmland", "orchard", "vineyard"}:
            counts["agriculture"] += 1
    return counts


def _land_cover_stats(elements: list[dict[str, object]]) -> dict[str, object]:
    hectares = {
        "urban_percent": 0.0,
        "forest_percent": 0.0,
        "field_plain_percent": 0.0,
        "agriculture_percent": 0.0,
        "water_percent": 0.0,
        "wetland_percent": 0.0,
    }
    feature_counts = dict.fromkeys(hectares, 0.0)
    for element in elements:
        tags = element.get("tags", {})
        if not isinstance(tags, dict):
            continue
        category = _land_cover_category(tags)
        if category is None:
            continue
        key = f"{category}_percent"
        feature_counts[key] += 1.0
        hectares[key] += _element_area_hectares(element)
    area_total = sum(hectares.values())
    if area_total > 0:
        return {
            "percentages": {key: round(value * 100 / area_total, 1) for key, value in hectares.items()},
            "hectares": {key.replace("_percent", "_ha"): round(value, 1) for key, value in hectares.items()},
            "basis": "osm_classified_area",
        }
    count_total = sum(feature_counts.values())
    return {
        "percentages": {
            key: round(value * 100 / count_total, 1) if count_total else 0.0
            for key, value in feature_counts.items()
        },
        "hectares": {key.replace("_percent", "_ha"): 0.0 for key in hectares},
        "basis": "osm_feature_count_proxy",
    }


def _land_cover_category(tags: dict[str, object]) -> str | None:
    landuse = str(tags.get("landuse", ""))
    natural = str(tags.get("natural", ""))
    if natural == "water" or landuse in {"reservoir", "basin"}:
        return "water"
    if natural == "wetland":
        return "wetland"
    if landuse in {"residential", "industrial", "commercial", "retail", "construction"}:
        return "urban"
    if landuse in {"farmland", "orchard", "vineyard"}:
        return "agriculture"
    if landuse == "forest" or natural == "wood":
        return "forest"
    if landuse in {"meadow", "grass"} or natural in {"grassland", "scrub"}:
        return "field_plain"
    return None


def _element_area_hectares(element: dict[str, object]) -> float:
    geometry = element.get("geometry")
    if not isinstance(geometry, list) or len(geometry) < 3:
        return 0.0
    points = []
    for point in geometry:
        if not isinstance(point, dict):
            continue
        try:
            points.append((float(point["lon"]), float(point["lat"])))
        except (KeyError, TypeError, ValueError):
            continue
    if len(points) < 3:
        return 0.0
    mean_lat = sum(point[1] for point in points) / len(points)
    km_per_degree_lon = 111.32 * max(0.2, math.cos(math.radians(mean_lat)))
    km_per_degree_lat = 110.57
    projected = [(lon * km_per_degree_lon, lat * km_per_degree_lat) for lon, lat in points]
    area_km2 = 0.0
    for index, point in enumerate(projected):
        next_point = projected[(index + 1) % len(projected)]
        area_km2 += (point[0] * next_point[1]) - (next_point[0] * point[1])
    return abs(area_km2) * 50.0


def _bbox_degrees2(west: float, south: float, east: float, north: float) -> float:
    return abs(east - west) * abs(north - south)


def _bbox_slug(config: MonitorConfig) -> str:
    west, south, east, north = bbox(config.polygon)
    return f"{west:.4f}-{south:.4f}-{east:.4f}-{north:.4f}"


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "selected-area"


def _read_json(path: Path, *, default: object) -> object:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))
