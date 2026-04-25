from __future__ import annotations

import argparse
import json

import logging  # Added for logger


from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from calamity_ai.config import load_config
from calamity_ai.copernicus import copernicus_to_dict, get_copernicus_summary
from calamity_ai.context import environmental_context_to_dict, get_environmental_context
from calamity_ai.delivery import write_site_bundle
from calamity_ai.forecast import get_open_meteo_predictions, predictions_to_dict
from calamity_ai.geo import bbox as polygon_bbox, centroid
from calamity_ai.resources import ensure_resources, resource_summary_to_dict
from calamity_ai.scoring import score_calamities
from calamity_ai.sensors import summarize_sensors
from calamity_ai.weather import (
    demo_weather_features,
    features_to_dict,
    get_local_weather_features,
    get_open_meteo_weather_features,
)


logger = logging.getLogger(__name__)  # Added logger definition

def get_risk_value(calamity_data: dict | float) -> float:
    """Safely extract risk value from calamity data (handles dict or float)."""
    if isinstance(calamity_data, dict):
        return calamity_data.get('risk_index_percent', 0.0)
    return float(calamity_data) if isinstance(calamity_data, (int, float)) else 0.0


def _to_plain_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if is_dataclass(value):
        return asdict(value)
    return {}

def build_report(args: argparse.Namespace) -> dict[str, object]:
    config = load_runtime_config(args)
    now = datetime.now(timezone.utc)
    
    # Load resources if not skipped
    resource_summary = None
    if not args.no_resources:
        try:
            resource_summary = ensure_resources(config)
            resource_summary = resource_summary_to_dict(resource_summary)
        except Exception as e:
            logger.warning(f"Resource loading error: {e}")
    
    # Get weather features
    try:
        if args.demo:
            features = demo_weather_features()
        elif args.provider == "local":
            features = get_local_weather_features(args.weather)
        elif args.provider == "openmeteo":
            features = get_open_meteo_weather_features(config)
        else:
            raise ValueError(f"Unsupported weather provider: {args.provider}")
    except Exception as e:
        logger.error(f"Weather features error: {e}")
        features = None
    
    if features is None:
        raise RuntimeError("Failed to load weather features")
    
    # Get context (historical data)
    context = None
    if not args.no_context:
        try:
            context = get_environmental_context(config, now=now)
            context = environmental_context_to_dict(context)
        except Exception as e:
            logger.warning(f"Context loading error: {e}")
    
    # Summarize sensors
    sensors = None
    try:
        sensors = summarize_sensors(
            args.sensors,
            now=now,
            min_online_ratio=getattr(config, 'sensor_health', {}).get("min_online_ratio", 0.5),
            stale_after_minutes=1e9 if args.demo else getattr(config, 'sensor_health', {}).get("stale_after_minutes", 3600),
        )
    except Exception as e:
        logger.warning(f"Sensor summary error: {e}")
    
    # Score calamities
    calamities = {}
    try:
        calamities = score_calamities(features, getattr(config, 'thresholds', {}), context=context)
    except Exception as e:
        logger.warning(f"Calamity scoring error: {e}")
    
    # Convert features to dict after scoring
    try:
        features = features_to_dict(features)
    except Exception as e:
        logger.warning(f"Features to dict error: {e}")
        features = {}
    
    # Predictions
    predictions = None
    if context and not args.no_predictions:
        try:
            predictions = get_open_meteo_predictions(config, context=context)
            predictions = predictions_to_dict(predictions)
        except Exception as e:
            logger.warning(f"Predictions error: {e}")
    
    # Copernicus
    copernicus = None
    if not args.no_copernicus:
        try:
            copernicus = get_copernicus_summary(config, now=now)
            copernicus = copernicus_to_dict(copernicus)
        except Exception as e:
            logger.warning(f"Copernicus error: {e}")
    
    # Extract risk stats safely
    risk_values = [get_risk_value(v) for v in calamities.values()] if calamities else []
    max_risk = max(risk_values) if risk_values else 0.0
    max_risk_name = max(calamities, key=lambda name: get_risk_value(calamities[name])) if calamities else "none"
    max_risk_level = calamities.get(max_risk_name, {}).get("risk", "low") if calamities else "low"
    active_alerts = len([v for v in risk_values if v > 30])
    min_lon, min_lat, max_lon, max_lat = polygon_bbox(config.polygon)
    center_lon, center_lat = centroid(config.polygon)
    sensors_payload = _to_plain_dict(sensors)
    elevated_risks = [k for k, v in calamities.items() if get_risk_value(v) > 40]
    notes = (
        f"Current elevated risks: {', '.join(elevated_risks) if elevated_risks else 'none'}; "
        f"sensors online: {sensors_payload.get('online', 0)}/{sensors_payload.get('total', 0)}"
    )
    
    # Build return dict with safe access
    return {
        "schema_version": "1.0",
        "timestamp": now.isoformat(),
        "generated_at": now.isoformat(),
        "title": "Calamity Intelligence Dashboard",
        "subtitle": f"{getattr(config, 'area_name', 'Unknown')} risk monitoring",
        "area": {
            "name": getattr(config, 'area_name', 'Unknown'),
            "bbox": [min_lon, min_lat, max_lon, max_lat],
            "center": [center_lon, center_lat],
        },
        "calamities": calamities,
        "sensors": sensors_payload,
        "context": context,
        "copernicus": copernicus,
        "resources": resource_summary,
        "forecast": predictions,
        "working": bool(sensors_payload.get("working", True)),
        "notes": notes,
        "stats": [
            {
                "id": "max-risk",
                "label": "Max risk",
                "value": max_risk,
                "unit": "%",
                "status": f"{max_risk_name}:{max_risk_level}" if calamities else "low",
            },
            {
                "id": "active-alerts",
                "label": "Active alerts",
                "value": active_alerts,
                "unit": "",
                "status": "active" if active_alerts > 0 else "none",
            },
            {
                "id": "sensors-online",
                "label": "Sensors online",
                "value": sensors_payload.get("online", 0),
                "unit": f"/{sensors_payload.get('total', 0)}",
                "status": "working" if sensors_payload.get("working") else "degraded",
            },
            {
                "id": "risk-area",
                "label": "Risk area",
                "value": 1,
                "unit": "",
                "status": "ready",
            }
        ],
        "weather": features,
        "alerts": [
            {
                "id": f"risk-{k}",
                "type": "risk",
                "severity": "medium" if get_risk_value(v) > 40 else "low",
                "title": k.capitalize(),
                "message": f"{k.capitalize()} risk is {get_risk_value(v)}% ({'medium' if get_risk_value(v) > 40 else 'low'})",
                "risk_index_percent": get_risk_value(v),
            }
            for k, v in calamities.items() if get_risk_value(v) > 0
        ] if calamities else [],
        "predictions": {
            "type": "area_summary",
            "area_count": 1,
            "indices_are_probabilities": False,
            "areas": [
                {
                    "id": "whole_surface",
                    "name": "Entire monitored area",
                    "max_risk_index": max_risk,
                    **{f"{k}_exposure_index": get_risk_value(v) for k, v in calamities.items()},
                    "most_relevant_risks": [k for k, v in calamities.items() if get_risk_value(v) == max_risk] if calamities else [],
                }
            ],
            "explanation": "Risk values are relative exposure indices for the entire monitored area, not probabilities and not official warning polygons.",
        },
        "map": {
            "center": [center_lon, center_lat],
            "bbox": [min_lon, min_lat, max_lon, max_lat],
            "layers": {
                "predictions": "processing/predictions.geojson",
                "events": "standardized/events.geojson",
                "maps": "standardized/maps.geojson",
                "weather": "standardized/weather.coverage.json",
                "satellite": "standardized/satellite.stac.json"
            }
        },
        "fields": [],
        "data_sources": [
            {
                "id": "weather",
                "label": "Open-Meteo / weather provider",
                "status": "ready"
            },
            {
                "id": "risk-model",
                "label": "Risk scoring model",
                "status": "ready"
            },
            {
                "id": "sentinel-1",
                "label": "Sentinel-1 radar",
                "status": "ready" if isinstance(copernicus, dict) and copernicus.get("sentinel1", {}).get("count", 0) else "empty",
                "products": copernicus.get("sentinel1", {}).get("count", 0) if isinstance(copernicus, dict) else 0
            },
            {
                "id": "sentinel-2",
                "label": "Sentinel-2 optical",
                "status": "ready" if isinstance(copernicus, dict) and copernicus.get("sentinel2", {}).get("count", 0) else "empty",
                "products": copernicus.get("sentinel2", {}).get("count", 0) if isinstance(copernicus, dict) else 0
            },
            {
                "id": "sensors",
                "label": "Local sensor inventory",
                "status": "ready" if sensors_payload.get("working") else "degraded",
                "online": sensors_payload.get("online", 0),
                "total": sensors_payload.get("total", 0)
            }
        ],
        "status": {
            "working": bool(sensors_payload.get("working", True)),
            "notes": notes
        }
    }


def _enrich_features_with_satellite(
    features: Any, copernicus: Any
) -> Any:
    """Merge satellite-derived indices into weather features dataclass."""
    from dataclasses import replace
    
    indices = copernicus.satellite_indices
    return replace(
        features,
        satellite_water_index=indices.water_index,
        satellite_soil_moisture_anomaly=indices.soil_moisture_anomaly,
        satellite_fire_radiative_power=indices.fire_radiative_power,
        satellite_burned_area_fraction=indices.burned_area_fraction,
        satellite_land_surface_temp_anomaly=indices.land_surface_temp_anomaly,
        satellite_ndvi_anomaly=indices.ndvi_anomaly,
        satellite_optical_quality=indices.optical_quality,
        satellite_radar_confidence=indices.radar_confidence,
    )


def load_runtime_config(args: argparse.Namespace) -> object:
    config = load_config(args.config)
    if args.bbox:
        config = config.with_area(args.area_name or "custom_selected_region", _bbox_to_polygon(args.bbox))
    return config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hourly AI-style calamity risk monitor")
    parser.add_argument("--config", default="config/monitor_config.json")
    parser.add_argument("--sensors", default="data/sensors.csv")
    parser.add_argument("--site-out", default="out/site")
    parser.add_argument("--weather", default="data/weather_features.json")
    parser.add_argument(
        "--bbox",
        nargs=4,
        type=float,
        metavar=("LON1", "LAT1", "LON2", "LAT2"),
        help="Analyze only the rectangle defined by two coordinate points.",
    )
    parser.add_argument("--area-name", help="Display name for --bbox selected region")
    parser.add_argument("--provider", choices=["openmeteo", "local"], default="openmeteo")
    parser.add_argument("--no-copernicus", action="store_true", help="Skip Copernicus satellite catalogue checks")
    parser.add_argument("--no-context", action="store_true", help="Skip historical weather and elevation context")
    parser.add_argument("--no-predictions", action="store_true", help="Skip multi-day forecast predictions")
    parser.add_argument("--no-resources", action="store_true", help="Skip OSM resource cache loading")
    parser.add_argument("--update-resources", action="store_true", help="Refresh cached OSM boundary and context resources")
    parser.add_argument("--prediction-days", type=int, default=5)
    parser.add_argument("--demo", action="store_true", help="Run with bundled demo weather values")
    parser.add_argument("--print-json", action="store_true", help="Print the full JSON report to console")
    parser.add_argument("--project", help="Project identifier to record in the config")
    parser.add_argument("--save-project", action="store_true", help="Persist project_id to config file")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    
    if args.project and args.save_project:
        config_path = Path(args.config)
        raw = json.loads(config_path.read_text(encoding="utf-8"))
        raw["project_id"] = args.project
        config_path.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")
    
    report = build_report(args)
    
    Path(args.site_out).mkdir(parents=True, exist_ok=True)
    write_site_bundle(report, load_runtime_config(args), args.site_out)
    
    if args.print_json:
        print(json.dumps(report, indent=2))

def _bbox_to_polygon(values: list[float]) -> list[list[float]]:
    lon1, lat1, lon2, lat2 = values
    west, east = sorted([lon1, lon2])
    south, north = sorted([lat1, lat2])
    if west == east or south == north:
        raise ValueError("--bbox needs two different longitude/latitude points.")
    return [
        [west, north],
        [east, north],
        [east, south],
        [west, south],
        [west, north],
    ]


if __name__ == "__main__":
    main()
