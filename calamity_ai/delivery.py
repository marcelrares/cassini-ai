from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .config import MonitorConfig
from .geo import bbox, centroid


def write_site_bundle(report: dict[str, object], config: MonitorConfig, output_dir: str | Path) -> dict[str, object]:
    base = Path(output_dir)
    paths = {
        "sources": base / "sources",
        "standardized": base / "standardized",
        "processing": base / "processing",
        "delivery": base,
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)

    files = {
        "sources_satellite": paths["sources"] / "satellite.raw.json",
        "sources_weather": paths["sources"] / "weather.raw.json",
        "sources_maps": paths["sources"] / "maps.raw.geojson",
        "sources_events": paths["sources"] / "events.raw.geojson",
        "standardized_satellite": paths["standardized"] / "satellite.stac.json",
        "standardized_weather": paths["standardized"] / "weather.coverage.json",
        "standardized_maps": paths["standardized"] / "maps.geojson",
        "standardized_events": paths["standardized"] / "events.geojson",
        "processing_predictions": paths["processing"] / "predictions.geojson",
        "manifest": paths["delivery"] / "manifest.json",
    }

    _write_json(files["sources_satellite"], _raw_satellite(report))
    _write_json(files["sources_weather"], _raw_weather(report))
    _write_json(files["sources_maps"], _maps_geojson(config, report))
    _write_json(files["sources_events"], _events_geojson(config, report, standardized=False))
    _write_json(files["standardized_satellite"], _satellite_stac(report))
    _write_json(files["standardized_weather"], _weather_coverage(config, report))
    _write_json(files["standardized_maps"], _maps_geojson(config, report))
    _write_json(files["standardized_events"], _events_geojson(config, report, standardized=True))
    _write_json(files["processing_predictions"], _predictions_geojson(config, report))
    manifest = _manifest(config, report, base, files)
    _write_json(files["manifest"], manifest)
    return manifest


def _manifest(
    config: MonitorConfig,
    report: dict[str, object],
    base: Path,
    files: dict[str, Path],
) -> dict[str, object]:
    min_lon, min_lat, max_lon, max_lat = bbox(config.polygon)
    return {
        "schema_version": "1.0",
        "generated_at": report.get("timestamp"),
        "area": {
            "name": config.area_name,
            "bbox": [min_lon, min_lat, max_lon, max_lat],
            "crs": "EPSG:4326",
        },
        "entrypoint": "manifest.json",
        "layers": [
            {
                "id": "satellite",
                "title": "Satellite evidence",
                "type": "stac",
                "role": "background",
                "href": _relative(base, files["standardized_satellite"]),
                "visible": True,
                "z_index": 10,
            },
            {
                "id": "weather",
                "title": "Weather coverage",
                "type": "coveragejson",
                "role": "overlay",
                "href": _relative(base, files["standardized_weather"]),
                "visible": True,
                "z_index": 20,
            },
            {
                "id": "maps",
                "title": "Area and local context",
                "type": "geojson",
                "role": "reference",
                "href": _relative(base, files["standardized_maps"]),
                "visible": True,
                "z_index": 30,
            },
            {
                "id": "predictions",
                "title": "Risk predictions",
                "type": "geojson",
                "role": "risk",
                "href": _relative(base, files["processing_predictions"]),
                "visible": True,
                "z_index": 40,
                "style": {
                    "property": "max_risk_index",
                    "stops": [
                        {"lte": 39.9, "color": "#2e7d32", "label": "low"},
                        {"lte": 69.9, "color": "#f9a825", "label": "medium"},
                        {"lte": 100, "color": "#c62828", "label": "high"},
                    ],
                },
            },
            {
                "id": "events",
                "title": "Events and system status",
                "type": "geojson",
                "role": "icons",
                "href": _relative(base, files["standardized_events"]),
                "visible": True,
                "z_index": 50,
            },
        ],
        "raw_sources": {
            "satellite": _relative(base, files["sources_satellite"]),
            "weather": _relative(base, files["sources_weather"]),
            "maps": _relative(base, files["sources_maps"]),
            "events": _relative(base, files["sources_events"]),
        },
        "status": {
            "working": report.get("working"),
            "notes": report.get("notes"),
        },
    }


def _raw_satellite(report: dict[str, object]) -> dict[str, object]:
    return {
        "stage": "sources",
        "source_type": "satellite",
        "data": report.get("copernicus"),
    }


def _raw_weather(report: dict[str, object]) -> dict[str, object]:
    return {
        "stage": "sources",
        "source_type": "weather",
        "data": report.get("weather"),
    }


def _satellite_stac(report: dict[str, object]) -> dict[str, object]:
    copernicus = report.get("copernicus")
    features: list[dict[str, object]] = []
    if isinstance(copernicus, dict):
        for key in ["sentinel1", "sentinel2"]:
            collection = copernicus.get(key)
            if isinstance(collection, dict):
                features.extend(_stac_items(collection))
        for collection in copernicus.get("auxiliary", []):
            if isinstance(collection, dict):
                features.extend(_stac_items(collection))
    return {
        "type": "FeatureCollection",
        "stac_version": "1.0.0",
        "features": features,
    }


def _stac_items(collection: dict[str, object]) -> list[dict[str, object]]:
    items = []
    collection_name = str(collection.get("collection", "unknown"))
    for product in collection.get("products", []):
        if not isinstance(product, dict):
            continue
        items.append(
            {
                "type": "Feature",
                "stac_version": "1.0.0",
                "id": product.get("id"),
                "collection": collection_name,
                "geometry": None,
                "bbox": None,
                "properties": {
                    "datetime": product.get("datetime"),
                    "platform": product.get("platform"),
                    "eo:cloud_cover": product.get("cloud_cover"),
                },
                "assets": {},
            }
        )
    return items


def _weather_coverage(config: MonitorConfig, report: dict[str, object]) -> dict[str, object]:
    weather = report.get("weather") if isinstance(report.get("weather"), dict) else {}
    center_lon, center_lat = centroid(config.polygon)
    return {
        "type": "Coverage",
        "domain": {
            "type": "Domain",
            "domainType": "PointSeries",
            "axes": {
                "x": {"values": [center_lon]},
                "y": {"values": [center_lat]},
                "t": {"values": [report.get("timestamp")]},
            },
            "referencing": [
                {
                    "coordinates": ["x", "y"],
                    "system": {"type": "GeographicCRS", "id": "http://www.opengis.net/def/crs/EPSG/0/4326"},
                },
                {
                    "coordinates": ["t"],
                    "system": {"type": "TemporalRS", "calendar": "Gregorian"},
                },
            ],
        },
        "parameters": {
            key: {"type": "Parameter", "description": key, "unit": _weather_unit(key)}
            for key in weather
        },
        "ranges": {
            key: {"type": "NdArray", "dataType": "float", "axisNames": ["t", "y", "x"], "shape": [1, 1, 1], "values": [value]}
            for key, value in weather.items()
        },
    }


def _maps_geojson(config: MonitorConfig, report: dict[str, object]) -> dict[str, object]:
    features = [
        {
            "type": "Feature",
            "id": "aoi",
            "geometry": {
                "type": "Polygon",
                "coordinates": [config.polygon],
            },
            "properties": {
                "name": config.area_name,
                "kind": "area_of_interest",
                "crs": "EPSG:4326",
            },
        }
    ]
    resources = report.get("resources")
    if isinstance(resources, dict):
        features.append(
            {
                "type": "Feature",
                "id": "resource-summary",
                "geometry": None,
                "properties": {
                    "kind": "resource_summary",
                    **resources,
                },
            }
        )
    return {"type": "FeatureCollection", "features": features}


def _events_geojson(config: MonitorConfig, report: dict[str, object], *, standardized: bool) -> dict[str, object]:
    lon, lat = centroid(config.polygon)
    features = []
    calamities = report.get("calamities")
    if isinstance(calamities, dict):
        for name, data in calamities.items():
            if not isinstance(data, dict) or data.get("risk") not in {"medium", "high"}:
                continue
            features.append(
                {
                    "type": "Feature",
                    "id": f"risk-{name}",
                    "geometry": {"type": "Point", "coordinates": [lon, lat]},
                    "properties": {
                        "event_type": "risk" if standardized else "raw_risk_signal",
                        "risk": name,
                        "level": data.get("risk"),
                        "risk_index_percent": data.get("risk_index_percent"),
                        "title": data.get("title"),
                    },
                }
            )
    sensors = report.get("sensors")
    if isinstance(sensors, dict) and int(sensors.get("offline", 0) or 0) > 0:
        features.append(
            {
                "type": "Feature",
                "id": "sensor-status",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {
                    "event_type": "sensor_status",
                    "offline": sensors.get("offline"),
                    "stale": sensors.get("stale"),
                    "working": sensors.get("working"),
                },
            }
        )
    return {"type": "FeatureCollection", "features": features}


def _predictions_geojson(config: MonitorConfig, report: dict[str, object]) -> dict[str, object]:
    zone_analysis = report.get("zone_analysis")
    if isinstance(zone_analysis, dict) and isinstance(zone_analysis.get("zones"), list):
        features = [_zone_prediction_feature(zone) for zone in zone_analysis["zones"] if isinstance(zone, dict)]
        return {"type": "FeatureCollection", "features": features}

    min_lon, min_lat, max_lon, max_lat = bbox(config.polygon)
    calamities = report.get("calamities") if isinstance(report.get("calamities"), dict) else {}
    risk_values = [
        float(data.get("risk_index_percent", 0))
        for data in calamities.values()
        if isinstance(data, dict)
    ]
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "id": "aoi-risk",
                "bbox": [min_lon, min_lat, max_lon, max_lat],
                "geometry": {"type": "Polygon", "coordinates": [config.polygon]},
                "properties": {
                    "name": config.area_name,
                    "max_risk_index": max(risk_values, default=0),
                    "risks": {
                        name: {
                            "risk": data.get("risk"),
                            "risk_index_percent": data.get("risk_index_percent"),
                        }
                        for name, data in calamities.items()
                        if isinstance(data, dict)
                    },
                },
            }
        ],
    }


def _zone_prediction_feature(zone: dict[str, object]) -> dict[str, object]:
    lon = float(zone["center_longitude"])
    lat = float(zone["center_latitude"])
    flood = float(zone.get("flood_exposure_index", 0))
    drought = float(zone.get("drought_exposure_index", 0))
    wildfire = float(zone.get("wildfire_exposure_index", 0))
    return {
        "type": "Feature",
        "id": zone.get("zone_id"),
        "geometry": zone.get("geometry") or {"type": "Point", "coordinates": [lon, lat]},
        "properties": {
            "name": zone.get("name"),
            "center_longitude": lon,
            "center_latitude": lat,
            "flood_exposure_index": flood,
            "drought_exposure_index": drought,
            "wildfire_exposure_index": wildfire,
            "max_risk_index": max(flood, drought, wildfire),
            "most_relevant_risks": zone.get("most_relevant_risks", []),
            "explanation": zone.get("explanation"),
        },
    }


def _weather_unit(key: str) -> str | None:
    if key.endswith("_c"):
        return "Cel"
    if key.endswith("_m"):
        return "m"
    if key.endswith("_mm"):
        return "mm"
    if key.endswith("_ms"):
        return "m/s"
    if key.endswith("_percent"):
        return "%"
    if key.endswith("_kpa"):
        return "kPa"
    return None


def _relative(base: Path, path: Path) -> str:
    return path.relative_to(base).as_posix()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
