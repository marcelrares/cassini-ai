from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .config import MonitorConfig
from .geo import bbox


RESOURCE_DIR = Path("data/resources")
BOUNDARY_PATH = RESOURCE_DIR / "iasi_boundary.geojson"
OSM_CONTEXT_PATH = RESOURCE_DIR / "iasi_osm_context.json"


@dataclass(frozen=True)
class ResourceSummary:
    boundary_source: str
    osm_source: str
    waterway_features: int
    water_body_features: int
    urban_landuse_features: int
    forest_or_green_features: int
    agriculture_features: int
    cached_files: list[str]
    explanation: str


def ensure_resources(config: MonitorConfig, *, update: bool = False) -> ResourceSummary:
    RESOURCE_DIR.mkdir(parents=True, exist_ok=True)
    if update or not BOUNDARY_PATH.exists():
        _fetch_boundary()
    if update or not OSM_CONTEXT_PATH.exists():
        _fetch_osm_context(config)
    return load_resource_summary()


def load_resource_summary() -> ResourceSummary:
    osm = _read_json(OSM_CONTEXT_PATH, default={"elements": []})
    counts = _feature_counts(osm.get("elements", []))
    return ResourceSummary(
        boundary_source="OpenStreetMap/Nominatim cached boundary" if BOUNDARY_PATH.exists() else "not available",
        osm_source="OpenStreetMap/Overpass cached extract" if OSM_CONTEXT_PATH.exists() else "not available",
        waterway_features=counts["waterway"],
        water_body_features=counts["water_body"],
        urban_landuse_features=counts["urban"],
        forest_or_green_features=counts["green"],
        agriculture_features=counts["agriculture"],
        cached_files=[str(path) for path in [BOUNDARY_PATH, OSM_CONTEXT_PATH] if path.exists()],
        explanation=(
            "Local resource cache adds hydrology, water bodies, urban/impervious proxies, vegetation/forest, and agriculture context. "
            "These features refine relative zone exposure; they are not official hazard maps."
        ),
    )


def resource_summary_to_dict(summary: ResourceSummary) -> dict[str, object]:
    return asdict(summary)


def load_osm_context() -> dict[str, object] | None:
    if not OSM_CONTEXT_PATH.exists():
        return None
    return _read_json(OSM_CONTEXT_PATH, default=None)


def _fetch_boundary() -> None:
    params = {
        "q": "Iasi, Romania",
        "format": "geojson",
        "polygon_geojson": 1,
        "limit": 1,
    }
    url = "https://nominatim.openstreetmap.org/search?" + urlencode(params)
    request = Request(url, headers={"User-Agent": "calamity-ai-monitor/1.0"})
    with urlopen(request, timeout=45) as response:
        payload = json.loads(response.read().decode("utf-8"))
    BOUNDARY_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _fetch_osm_context(config: MonitorConfig) -> None:
    west, south, east, north = bbox(config.polygon)
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
    OSM_CONTEXT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


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

def _read_json(path: Path, *, default: object) -> object:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))
