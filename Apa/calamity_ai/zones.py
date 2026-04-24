from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from statistics import mean
from urllib.parse import urlencode
from urllib.request import urlopen

from .config import MonitorConfig
from .resources import load_osm_context
from .scoring import WeatherFeatures, clamp01


@dataclass(frozen=True)
class ZoneExposure:
    zone_id: str
    name: str
    center_latitude: float
    center_longitude: float
    elevation_m: float | None
    flood_exposure_index: float
    drought_exposure_index: float
    wildfire_exposure_index: float
    most_relevant_risks: list[str]
    explanation: str


@dataclass(frozen=True)
class ZoneAnalysis:
    method: str
    indices_are_probabilities: bool
    zones: list[ZoneExposure]
    top_flood_zones: list[ZoneExposure]
    top_drought_zones: list[ZoneExposure]
    top_wildfire_zones: list[ZoneExposure]
    explanation: str


def get_zone_analysis(
    config: MonitorConfig,
    *,
    features: WeatherFeatures,
    calamities: dict[str, dict[str, object]],
    context: object | None,
) -> ZoneAnalysis:
    rows = int(config.zones.get("rows", 3))
    cols = int(config.zones.get("cols", 3))
    centers = _grid_centers(config.polygon, rows, cols)
    elevations = _load_elevations(centers)
    osm_context = load_osm_context()
    min_elev = min((value for value in elevations if value is not None), default=None)
    max_elev = max((value for value in elevations if value is not None), default=None)
    dryness = _historical_dryness(context)
    wetness = _historical_wetness(context)
    terrain_class = _terrain_class(context)

    zones: list[ZoneExposure] = []
    for index, ((lon, lat), elevation) in enumerate(zip(centers, elevations), start=1):
        low_elevation_factor = 0.5
        high_elevation_factor = 0.5
        if elevation is not None and min_elev is not None and max_elev is not None and max_elev > min_elev:
            low_elevation_factor = 1 - ((elevation - min_elev) / (max_elev - min_elev))
            high_elevation_factor = 1 - low_elevation_factor
        resource_factors = _resource_factors(osm_context, lon=lon, lat=lat)
        flood_base = float(calamities["flood"]["score"])
        drought_base = float(calamities["drought"]["score"])
        wildfire_base = float(calamities["wildfire"]["score"])
        flood_exposure = clamp01(
            flood_base
            * (
                0.62
                + (0.24 * low_elevation_factor)
                + (0.12 * wetness)
                + (0.08 * _terrain_runoff(terrain_class))
                + (0.18 * resource_factors["water_proximity"])
                + (0.08 * resource_factors["urban_proximity"])
            )
        )
        drought_exposure = clamp01(
            drought_base
            * (
                0.70
                + (0.22 * dryness)
                + (0.10 * high_elevation_factor)
                + (0.10 * _soil_dryness(features))
                + (0.08 * resource_factors["agriculture_proximity"])
            )
        )
        wildfire_exposure = clamp01(
            wildfire_base
            * (
                0.64
                + (0.18 * dryness)
                + (0.08 * high_elevation_factor)
                + (0.10 * _ignition_weather(features))
                + (0.14 * resource_factors["vegetation_proximity"])
                + (0.06 * resource_factors["agriculture_proximity"])
            )
        )
        zone = ZoneExposure(
            zone_id=f"Z{index}",
            name=_zone_name(config, index - 1, rows, cols),
            center_latitude=round(lat, 5),
            center_longitude=round(lon, 5),
            elevation_m=None if elevation is None else round(elevation, 1),
            flood_exposure_index=round(flood_exposure * 100, 1),
            drought_exposure_index=round(drought_exposure * 100, 1),
            wildfire_exposure_index=round(wildfire_exposure * 100, 1),
            most_relevant_risks=_top_risks(flood_exposure, drought_exposure, wildfire_exposure),
            explanation=_zone_explanation(
                elevation,
                low_elevation_factor,
                high_elevation_factor,
                flood_exposure,
                drought_exposure,
                wildfire_exposure,
                resource_factors,
            ),
        )
        zones.append(zone)

    return ZoneAnalysis(
        method="Named Iasi AOI sectors using elevation samples, OSM hydrology/land-cover context, recent weather, historical baseline factors, and global AOI risk indices",
        indices_are_probabilities=False,
        zones=zones,
        top_flood_zones=sorted(zones, key=lambda item: item.flood_exposure_index, reverse=True)[:3],
        top_drought_zones=sorted(zones, key=lambda item: item.drought_exposure_index, reverse=True)[:3],
        top_wildfire_zones=sorted(zones, key=lambda item: item.wildfire_exposure_index, reverse=True)[:3],
        explanation=(
            "Zone values are relative exposure indices, not probabilities and not official warning polygons. They are anchored to the overall AOI hazard level, "
            "then adjusted by local terrain. A higher zone value means this sector is more exposed than other monitored sectors if that hazard materializes."
        ),
    )


def zone_analysis_to_dict(analysis: ZoneAnalysis) -> dict[str, object]:
    return asdict(analysis)


def _grid_centers(polygon: list[list[float]], rows: int, cols: int) -> list[tuple[float, float]]:
    longitudes = [point[0] for point in polygon]
    latitudes = [point[1] for point in polygon]
    min_lon, max_lon = min(longitudes), max(longitudes)
    min_lat, max_lat = min(latitudes), max(latitudes)
    centers = []
    for row in range(rows):
        for col in range(cols):
            lon = min_lon + ((col + 0.5) / cols) * (max_lon - min_lon)
            lat = max_lat - ((row + 0.5) / rows) * (max_lat - min_lat)
            centers.append((lon, lat))
    return centers


def _load_elevations(points: list[tuple[float, float]]) -> list[float | None]:
    params = {
        "latitude": ",".join(str(point[1]) for point in points),
        "longitude": ",".join(str(point[0]) for point in points),
    }
    try:
        with urlopen("https://api.open-meteo.com/v1/elevation?" + urlencode(params), timeout=45) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return [None if value is None else float(value) for value in payload.get("elevation", [])]
    except Exception:
        return [None for _ in points]


def _resource_factors(osm_context: dict[str, object] | None, *, lon: float, lat: float) -> dict[str, float]:
    if not osm_context:
        return {
            "water_proximity": 0.0,
            "urban_proximity": 0.0,
            "vegetation_proximity": 0.0,
            "agriculture_proximity": 0.0,
        }
    features = _feature_points(osm_context)
    return {
        "water_proximity": _proximity(features["water"], lon, lat, decay_km=1.2),
        "urban_proximity": _proximity(features["urban"], lon, lat, decay_km=1.0),
        "vegetation_proximity": _proximity(features["green"], lon, lat, decay_km=1.5),
        "agriculture_proximity": _proximity(features["agriculture"], lon, lat, decay_km=2.0),
    }


def _feature_points(osm_context: dict[str, object]) -> dict[str, list[tuple[float, float]]]:
    grouped = {"water": [], "urban": [], "green": [], "agriculture": []}
    for element in osm_context.get("elements", []):
        if not isinstance(element, dict):
            continue
        tags = element.get("tags", {})
        if not isinstance(tags, dict):
            continue
        centroid = _element_centroid(element)
        if centroid is None:
            continue
        if "waterway" in tags or tags.get("natural") == "water" or tags.get("landuse") in {"reservoir", "basin"}:
            grouped["water"].append(centroid)
        if tags.get("landuse") in {"residential", "industrial", "commercial", "retail", "construction"}:
            grouped["urban"].append(centroid)
        if tags.get("landuse") in {"forest", "meadow", "grass"} or tags.get("natural") in {"wood", "scrub", "grassland", "wetland"}:
            grouped["green"].append(centroid)
        if tags.get("landuse") in {"farmland", "orchard", "vineyard"}:
            grouped["agriculture"].append(centroid)
    return grouped


def _element_centroid(element: dict[str, object]) -> tuple[float, float] | None:
    points: list[tuple[float, float]] = []
    geometry = element.get("geometry", [])
    if isinstance(geometry, list):
        for point in geometry:
            if isinstance(point, dict) and "lon" in point and "lat" in point:
                points.append((float(point["lon"]), float(point["lat"])))
    members = element.get("members", [])
    if isinstance(members, list):
        for member in members:
            if isinstance(member, dict):
                member_geometry = member.get("geometry", [])
                if isinstance(member_geometry, list):
                    for point in member_geometry:
                        if isinstance(point, dict) and "lon" in point and "lat" in point:
                            points.append((float(point["lon"]), float(point["lat"])))
    if not points:
        return None
    return mean(point[0] for point in points), mean(point[1] for point in points)


def _proximity(points: list[tuple[float, float]], lon: float, lat: float, *, decay_km: float) -> float:
    if not points:
        return 0.0
    distance = min(_distance_km(lon, lat, point[0], point[1]) for point in points)
    return clamp01(1 - (distance / decay_km))


def _distance_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    mean_lat = (lat1 + lat2) / 2
    km_per_degree_lon = 111.32 * max(0.2, math.cos(math.radians(mean_lat)))
    dx = (lon1 - lon2) * km_per_degree_lon
    dy = (lat1 - lat2) * 110.57
    return ((dx * dx) + (dy * dy)) ** 0.5


def _historical_dryness(context: object | None) -> float:
    baseline = getattr(getattr(context, "history", None), "seasonal_baseline", None)
    precip_30 = getattr(baseline, "precipitation_30d_ratio", None)
    precip_90 = getattr(baseline, "precipitation_90d_ratio", None)
    dry_90 = getattr(baseline, "dry_days_90d_ratio", None)
    rainfall_deficit = 0.0 if precip_30 is None else clamp01((1 - float(precip_30)) / 0.8)
    long_deficit = 0.0 if precip_90 is None else clamp01((1 - float(precip_90)) / 0.8)
    dry_days = 0.0 if dry_90 is None else clamp01((float(dry_90) - 0.9) / 0.8)
    return clamp01((0.45 * rainfall_deficit) + (0.35 * long_deficit) + (0.20 * dry_days))


def _historical_wetness(context: object | None) -> float:
    baseline = getattr(getattr(context, "history", None), "seasonal_baseline", None)
    precip_30 = getattr(baseline, "precipitation_30d_ratio", None)
    precip_90 = getattr(baseline, "precipitation_90d_ratio", None)
    wet_30 = 0.0 if precip_30 is None else clamp01((float(precip_30) - 1.0) / 1.0)
    wet_90 = 0.0 if precip_90 is None else clamp01((float(precip_90) - 1.0) / 1.0)
    return clamp01((0.65 * wet_30) + (0.35 * wet_90))


def _terrain_class(context: object | None) -> str:
    return str(getattr(getattr(context, "elevation", None), "terrain_class", "unknown"))


def _terrain_runoff(terrain_class: str) -> float:
    if terrain_class == "steep":
        return 0.8
    if terrain_class == "rolling":
        return 0.45
    if terrain_class == "flat":
        return 0.25
    return 0.3


def _soil_dryness(features: WeatherFeatures) -> float:
    if features.soil_moisture_proxy is None:
        return 0.5
    return 1 - clamp01(features.soil_moisture_proxy)


def _ignition_weather(features: WeatherFeatures) -> float:
    heat = clamp01((features.temp_max_24h_c - 25) / 10)
    humidity = 0.5 if features.relative_humidity_mean_percent is None else 1 - clamp01(features.relative_humidity_mean_percent / 70)
    vpd = 0.0 if features.vapor_pressure_deficit_kpa is None else clamp01(features.vapor_pressure_deficit_kpa / 1.6)
    return clamp01((0.45 * heat) + (0.30 * humidity) + (0.25 * vpd))


def _top_risks(flood: float, drought: float, wildfire: float) -> list[str]:
    pairs = [
        ("flood", flood),
        ("drought", drought),
        ("wildfire", wildfire),
    ]
    return [name for name, value in sorted(pairs, key=lambda item: item[1], reverse=True) if value >= 0.25][:2]


def _zone_name(config: MonitorConfig, index: int, rows: int, cols: int) -> str:
    labels = config.zones.get("labels", [])
    if isinstance(labels, list) and index < len(labels):
        return str(labels[index])
    row = index // cols
    col = index % cols
    vertical = ["north", "central", "south"][row] if rows == 3 else f"row {row + 1}"
    horizontal = ["west", "central", "east"][col] if cols == 3 else f"column {col + 1}"
    if vertical == "central" and horizontal == "central":
        return "central sector"
    return f"{vertical}-{horizontal} sector"


def _zone_explanation(
    elevation: float | None,
    low_elevation_factor: float,
    high_elevation_factor: float,
    flood: float,
    drought: float,
    wildfire: float,
    resource_factors: dict[str, float],
) -> str:
    elevation_text = "unknown elevation" if elevation is None else f"sampled elevation about {round(elevation, 1)} m"
    return (
        f"This sector has {elevation_text}. Low-elevation factor is {round(low_elevation_factor, 2)} and high-ground/dryness factor is "
        f"{round(high_elevation_factor, 2)}. These are relative exposure indices, not event probabilities: flood {round(flood * 100, 1)}, "
        f"drought {round(drought * 100, 1)}, wildfire {round(wildfire * 100, 1)}. OSM resource factors: "
        f"water proximity {round(resource_factors['water_proximity'], 2)}, urban/impervious proxy {round(resource_factors['urban_proximity'], 2)}, "
        f"vegetation proxy {round(resource_factors['vegetation_proximity'], 2)}, agriculture proxy {round(resource_factors['agriculture_proximity'], 2)}."
    )
