from __future__ import annotations

import json
import math
from datetime import datetime, timezone
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
        "dashboard": paths["delivery"] / "dashboard.json",
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
    _write_json(files["dashboard"], _dashboard_json(config, report, base, files))
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
        "dashboard": {
            "href": _relative(base, files["dashboard"]),
            "description": "Compact dashboard payload for current risks, weather, sensors, and map widgets.",
        },
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


def _dashboard_json(
    config: MonitorConfig,
    report: dict[str, object],
    base: Path,
    files: dict[str, Path],
) -> dict[str, object]:
    calamities = report.get("calamities") if isinstance(report.get("calamities"), dict) else {}
    weather = report.get("weather") if isinstance(report.get("weather"), dict) else {}
    sensors = report.get("sensors") if isinstance(report.get("sensors"), dict) else {}
    risks = _risks(calamities)
    risk_counts = _risk_counts(risks)
    min_lon, min_lat, max_lon, max_lat = bbox(config.polygon)
    return {
        "schema_version": "1.0",
        "generated_at": _generated_at(report),
        "title": "Calamity Intelligence Dashboard",
        "subtitle": f"{config.area_name} risk monitoring",
        "area": {
            "name": config.area_name,
            "bbox": [min_lon, min_lat, max_lon, max_lat],
            "total_monitored_hectares": _area_hectares(config.polygon),
        },
        "stats": _dashboard_stats(risks, sensors),
        "weather": _dashboard_weather(weather),
        "alerts": _dashboard_alerts(risks, sensors),
        "predictions": _dashboard_predictions(risks),
        "map": {
            "center": list(centroid(config.polygon)),
            "bbox": [min_lon, min_lat, max_lon, max_lat],
            "layers": {
                "predictions": _relative(base, files["processing_predictions"]),
                "events": _relative(base, files["standardized_events"]),
                "maps": _relative(base, files["standardized_maps"]),
                "weather": _relative(base, files["standardized_weather"]),
                "satellite": _relative(base, files["standardized_satellite"]),
            },
        },
        "fields": [],
        "data_sources": _dashboard_sources(report),
        "status": {
            "working": report.get("working"),
            "notes": report.get("notes"),
        },
        "risks": risks,
        "risk_counts": risk_counts,
        "sensors": _sensor_summary(sensors),
        "terrain": _terrain_summary(report.get("context")),
        "resources": _resource_summary(report.get("resources")),
        "satellite_signals": _satellite_signals(weather),
        "environment": _environment_summary(weather),
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


def _risks(calamities: object) -> list[dict[str, object]]:
    if not isinstance(calamities, dict):
        return []
    risks = []
    for name, data in calamities.items():
        if not isinstance(data, dict):
            continue
        risks.append(
            {
                "id": str(name),
                "label": str(data.get("title", name)).title(),
                "risk": data.get("risk"),
                "risk_index_percent": data.get("risk_index_percent", data.get("risk_percent")),
                "score": data.get("score"),
                "is_probability": data.get("is_probability", False),
                "message": _risk_message(name, data),
            }
        )
    return sorted(
        risks,
        key=lambda item: float(item.get("risk_index_percent") or 0),
        reverse=True,
    )


def _risk_message(name: object, data: dict[str, object]) -> str:
    label = str(data.get("title", name)).title()
    value = data.get("risk_index_percent", data.get("risk_percent", 0))
    risk = data.get("risk", "unknown")
    return f"{label} risk is {value}% ({risk})."


def _risk_counts(risks: list[dict[str, object]]) -> dict[str, int]:
    counts = {"low": 0, "medium": 0, "high": 0}
    for risk in risks:
        label = str(risk.get("risk", "low"))
        if label in counts:
            counts[label] += 1
    return counts


def _generated_at(report: dict[str, object]) -> str:
    value = report.get("timestamp") or report.get("generated_at")
    if value:
        return str(value)
    return datetime.now(timezone.utc).isoformat()


def _dashboard_stats(risks: list[dict[str, object]], sensors: object) -> list[dict[str, object]]:
    highest = risks[0] if risks else {}
    active_alerts = [risk for risk in risks if risk.get("risk") in {"medium", "high"}]
    sensor_summary = _sensor_summary(sensors)
    return [
        {
            "id": "max-risk",
            "label": "Max risk",
            "value": highest.get("risk_index_percent", 0),
            "unit": "%",
            "status": f"{highest.get('id')}:{highest.get('risk')}" if highest else "unknown",
        },
        {
            "id": "active-alerts",
            "label": "Active alerts",
            "value": len(active_alerts),
            "unit": "",
            "status": "active" if active_alerts else "clear",
        },
        {
            "id": "sensors-online",
            "label": "Sensors online",
            "value": sensor_summary["online"],
            "unit": f"/{sensor_summary['total']}",
            "status": "working" if sensor_summary["working"] else "degraded",
        },
        {
            "id": "prediction-zones",
            "label": "Prediction zones",
            "value": 1,
            "unit": "",
            "status": "ready",
        },
    ]


def _dashboard_weather(weather: object) -> dict[str, object]:
    if not isinstance(weather, dict):
        return {}
    return {
        "temperature_current_c": weather.get("temp_current_c"),
        "temperature_max_last_24h_c": weather.get("temp_max_24h_c"),
        "temperature_forecast_max_next_24h_c": weather.get("temp_forecast_max_next_24h_c"),
        "precipitation_next_24h_mm": _meters_to_mm(weather.get("precip_24h_m")),
        "wind_gust_max_ms": weather.get("wind_gust_max_ms"),
        "relative_humidity_percent": weather.get("relative_humidity_mean_percent"),
        "soil_moisture_proxy": weather.get("soil_moisture_proxy"),
        "vapor_pressure_deficit_kpa": weather.get("vapor_pressure_deficit_kpa"),
        "evapotranspiration_24h_mm": weather.get("evapotranspiration_24h_mm"),
        "cape_max_jkg": weather.get("cape_max_jkg"),
    }


def _dashboard_alerts(risks: list[dict[str, object]], sensors: object) -> list[dict[str, object]]:
    alerts = [
        {
            "id": f"risk-{risk['id']}",
            "type": "risk",
            "severity": risk.get("risk"),
            "title": risk.get("label"),
            "message": risk.get("message"),
            "risk_index_percent": risk.get("risk_index_percent"),
        }
        for risk in risks
        if risk.get("risk") in {"medium", "high"}
    ]
    sensor_summary = _sensor_summary(sensors)
    if sensor_summary["offline"] > 0:
        alerts.append(
            {
                "id": "sensor-status",
                "type": "sensor",
                "severity": "medium",
                "title": "Sensor status degraded",
                "message": f"{sensor_summary['offline']} sensors offline or stale.",
            }
        )
    return alerts


def _dashboard_predictions(risks: list[dict[str, object]]) -> dict[str, object]:
    return {
        "type": "area_summary",
        "zone_count": 1,
        "indices_are_probabilities": False,
        "top_zones": [],
        "risks": {
            str(risk["id"]): {
                "risk": risk.get("risk"),
                "risk_index_percent": risk.get("risk_index_percent"),
            }
            for risk in risks
        },
    }


def _satellite_signals(weather: object) -> dict[str, object]:
    if not isinstance(weather, dict):
        return {"available": False, "signals": []}
    labels = {
        "satellite_water_index": "Water detection",
        "satellite_soil_moisture_anomaly": "Soil moisture anomaly",
        "satellite_fire_radiative_power": "Fire radiative power",
        "satellite_burned_area_fraction": "Burned area",
        "satellite_land_surface_temp_anomaly": "Land surface temperature anomaly",
        "satellite_ndvi_anomaly": "Vegetation anomaly",
        "satellite_optical_quality": "Optical quality",
        "satellite_radar_confidence": "Radar confidence",
    }
    signals = [
        {"id": key, "label": label, "value": weather.get(key)}
        for key, label in labels.items()
        if weather.get(key) is not None
    ]
    return {"available": bool(signals), "signals": signals}


def _environment_summary(weather: object) -> dict[str, object]:
    if not isinstance(weather, dict):
        return {"available": False}
    return {
        "available": True,
        "soil": {
            "moisture_proxy": weather.get("soil_moisture_proxy"),
            "status": _soil_status(weather.get("soil_moisture_proxy")),
        },
        "water_balance": {
            "precipitation_next_24h_mm": _meters_to_mm(weather.get("precip_24h_m")),
            "evapotranspiration_24h_mm": weather.get("evapotranspiration_24h_mm"),
            "vapor_pressure_deficit_kpa": weather.get("vapor_pressure_deficit_kpa"),
            "relative_humidity_percent": weather.get("relative_humidity_mean_percent"),
        },
        "storm_conditions": {
            "wind_gust_max_ms": weather.get("wind_gust_max_ms"),
            "cape_max_jkg": weather.get("cape_max_jkg"),
        },
    }


def _soil_status(value: object) -> str | None:
    if value is None:
        return None
    moisture = float(value)
    if moisture < 0.25:
        return "dry"
    if moisture < 0.55:
        return "moderate"
    return "wet"


def _sensor_summary(sensors: object) -> dict[str, object]:
    if not isinstance(sensors, dict):
        return {"total": 0, "online": 0, "offline": 0, "stale": 0, "working": False, "online_ratio": 0}
    total = int(sensors.get("total", 0) or 0)
    online = int(sensors.get("online", 0) or 0)
    return {
        "total": total,
        "online": online,
        "offline": int(sensors.get("offline", 0) or 0),
        "stale": int(sensors.get("stale", 0) or 0),
        "working": bool(sensors.get("working")),
        "online_ratio": round(online / total, 2) if total else 0,
    }


def _terrain_summary(context: object) -> dict[str, object]:
    if not isinstance(context, dict):
        return {"available": False}
    elevation = context.get("elevation") if isinstance(context.get("elevation"), dict) else {}
    return {
        "available": bool(elevation),
        "center_elevation_m": elevation.get("center_elevation_m"),
        "min_elevation_m": elevation.get("min_elevation_m"),
        "max_elevation_m": elevation.get("max_elevation_m"),
        "elevation_range_m": elevation.get("elevation_range_m"),
        "terrain_class": elevation.get("terrain_class"),
    }


def _resource_summary(resources: object) -> dict[str, object]:
    if not isinstance(resources, dict):
        return {"available": False}
    return {"available": True, **resources}


def _dashboard_sources(report: dict[str, object]) -> list[dict[str, object]]:
    sources = [
        {"id": "weather", "label": "Open-Meteo / weather provider", "status": "ready"},
        {"id": "risk-model", "label": "Risk scoring model", "status": "ready"},
    ]
    copernicus = report.get("copernicus")
    if isinstance(copernicus, dict):
        sources.append(
            {
                "id": "sentinel-1",
                "label": "Sentinel-1 radar",
                "status": "ready" if _collection_count(copernicus.get("sentinel1")) > 0 else "empty",
                "products": _collection_count(copernicus.get("sentinel1")),
            }
        )
        sources.append(
            {
                "id": "sentinel-2",
                "label": "Sentinel-2 optical",
                "status": "ready" if _collection_count(copernicus.get("sentinel2")) > 0 else "empty",
                "products": _collection_count(copernicus.get("sentinel2")),
            }
        )
    sensors = report.get("sensors")
    if isinstance(sensors, dict):
        sources.append(
            {
                "id": "sensors",
                "label": "Local sensor inventory",
                "status": "ready" if sensors.get("working") else "degraded",
            }
        )
    return sources


def _collection_count(collection: object) -> int:
    if not isinstance(collection, dict):
        return 0
    return int(collection.get("count", 0) or 0)


def _meters_to_mm(value: object) -> float | None:
    if value is None:
        return None
    return round(float(value) * 1000, 1)


def _area_hectares(polygon: list[list[float]]) -> float:
    if len(polygon) < 3:
        return 0.0
    mean_lat = sum(point[1] for point in polygon) / len(polygon)
    km_per_degree_lon = 111.32
    km_per_degree_lat = 110.57
    scale_x = km_per_degree_lon * max(0.2, math.cos(math.radians(mean_lat)))
    points = [(point[0] * scale_x, point[1] * km_per_degree_lat) for point in polygon]
    area_km2 = 0.0
    for index, point in enumerate(points):
        next_point = points[(index + 1) % len(points)]
        area_km2 += (point[0] * next_point[1]) - (next_point[0] * point[1])
    return round(abs(area_km2) * 50, 1)


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
