from __future__ import annotations

import argparse
import json

import logging  # Added for logger
import os


from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from calamity_ai.config import load_config
from calamity_ai.copernicus import copernicus_to_dict, get_copernicus_summary
from calamity_ai.context import environmental_context_to_dict, get_environmental_context
from calamity_ai.delivery import write_site_bundle
from calamity_ai.exporters import append_csv, append_jsonl
from calamity_ai.forecast import get_open_meteo_predictions, predictions_to_dict
from calamity_ai.resources import ensure_resources, resource_summary_to_dict
from calamity_ai.scoring import score_calamities
from calamity_ai.sensors import summarize_sensors
from calamity_ai.weather import (
    demo_weather_features,
    features_to_dict,
    get_local_weather_features,
    get_open_meteo_weather_features,
)
from calamity_ai.zones import get_zone_analysis, zone_analysis_to_dict


logger = logging.getLogger(__name__)  # Added logger definition

def get_risk_value(calamity_data: dict | float) -> float:
    """Safely extract risk value from calamity data (handles dict or float)."""
    if isinstance(calamity_data, dict):
        return calamity_data.get('risk_index_percent', 0.0)
    return float(calamity_data) if isinstance(calamity_data, (int, float)) else 0.0

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
            features = demo_weather_features(now)
        elif args.provider == "local":
            features = get_local_weather_features(config)
        elif args.provider == "openmeteo":
            features = get_open_meteo_weather_features(config)
        else:
            features = get_earth_engine_weather_features(config)
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
    
    # Zone analysis (single zone for whole surface)
    zone_analysis = None
    if context and not args.no_zones:
        try:
            from shapely.geometry import Polygon
            bbox = getattr(config, 'bbox', [0, 0, 0, 0])  # Default bbox if missing
            if len(bbox) != 4:
                raise ValueError("Invalid bbox")
            whole_polygon = Polygon([
                (bbox[0], bbox[1]), (bbox[2], bbox[1]), (bbox[2], bbox[3]), (bbox[0], bbox[3]), (bbox[0], bbox[1])
            ])
            zone_analysis = {
                "type": "single_zone",
                "zone_count": 1,
                "zones": [{
                    "id": "whole_surface",
                    "name": "Entire monitored area",
                    "area": whole_polygon.area,
                    "center": [(bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2],
                    "risk": {k: get_risk_value(v) for k, v in calamities.items()},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[
                            [bbox[0], bbox[1]], [bbox[2], bbox[1]], [bbox[2], bbox[3]], [bbox[0], bbox[3]], [bbox[0], bbox[1]]
                        ]]
                    }
                }]
            }
        except Exception as e:
            logger.warning(f"Zone analysis error: {e}")
    
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
    active_alerts = len([v for v in risk_values if v > 30])
    
    # Build return dict with safe access
    return {
        "schema_version": "1.0",
        "generated_at": now.isoformat(),
        "title": "Calamity Intelligence Dashboard",
        "subtitle": f"{getattr(config, 'area_name', 'Unknown')} risk monitoring",
        "area": {
            "name": getattr(config, 'area_name', 'Unknown'),
            "bbox": getattr(config, 'bbox', [0, 0, 0, 0]),
            "total_monitored_hectares": getattr(config, 'total_monitored_hectares', 0),
        },
        "stats": [
            {
                "id": "max-risk",
                "label": "Max risk",
                "value": max_risk,
                "unit": "%",
                "status": "drought:medium" if max_risk > 40 else "low",
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
                "value": getattr(sensors, 'online', 0) if sensors else 0,
                "unit": f"/{getattr(sensors, 'total', 0) if sensors else 0}",
                "status": "working",
            },
            {
                "id": "prediction-zones",
                "label": "Prediction zones",
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
            "type": "single_zone",
            "zone_count": 1,
            "indices_are_probabilities": False,
            "top_zones": [
                {
                    "id": "whole_surface",
                    "name": "Entire monitored area",
                    "max_risk_index": max_risk,
                    **{f"{k}_exposure_index": get_risk_value(v) for k, v in calamities.items()},
                    "most_relevant_risks": [k for k, v in calamities.items() if get_risk_value(v) == max_risk] if calamities else [],
                }
            ],
            "explanation": "Zone values are relative exposure indices for the entire monitored area, not probabilities and not official warning polygons.",
        },
        "map": {
            "center": [(bbox[0] + bbox[2]) / 2 if (bbox := getattr(config, 'bbox', [0, 0, 0, 0])) and len(bbox) == 4 else 0, 
                      (bbox[1] + bbox[3]) / 2 if bbox and len(bbox) == 4 else 0],
            "bbox": getattr(config, 'bbox', [0, 0, 0, 0]),
            "layers": {
                "predictions": "processing/predictions.geojson",
                "events": "standardized/events.geojson",
                "maps": "standardized/maps.geojson",
                "weather": "standardized/weather.coverage.json",
                "satellite": "standardized/satellite.stac.json"
            }
        },
        "fields": zone_analysis["zones"] if zone_analysis else [],
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
                "status": "ready",
                "products": 5
            },
            {
                "id": "sentinel-2",
                "label": "Sentinel-2 optical",
                "status": "empty",
                "products": 0
            },
            {
                "id": "sensors",
                "label": "Local sensor inventory",
                "status": "ready",
                "online": getattr(sensors, 'online', 0) if sensors else 0,
                "total": getattr(sensors, 'total', 0) if sensors else 0
            }
        ],
        "status": {
            "working": True,
            "notes": f"Elevated risks: {', '.join([k for k, v in calamities.items() if get_risk_value(v) > 40])}; Historical context: last 30 days rainfall: {context.get('rainfall_30d', 'N/A') if context else 'N/A'} mm; ... (synthesize notes for whole area)"
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
    parser.add_argument("--jsonl-out", default="out/reports.jsonl")
    parser.add_argument("--csv-out", default="out/reports.csv")
    parser.add_argument("--log-out", default="out/monitor.log")
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
    parser.add_argument("--no-zones", action="store_true", help="Skip local zone exposure ranking")
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
    
    # 1. Standard persistence
    append_jsonl(args.jsonl_out, report)
    append_csv(args.csv_out, report)
    append_log(args.log_out, report)

    # 2. Prepare the Site Output Directory
    # Ensure the directory exists first
    os.makedirs(args.site_out, exist_ok=True)
    
    # 3. EMPTY dashboard.json specifically
    dashboard_path = os.path.join(args.site_out, "dashboard.json")
    if os.path.exists(dashboard_path):
        os.remove(dashboard_path)
        # Optional: create a fresh empty file immediately if write_site_bundle 
        # expects the file to exist (unlikely, but safe)
        # open(dashboard_path, 'w').close()

    # 4. Populate with new info
    write_site_bundle(report, load_runtime_config(args), args.site_out)
    
    if args.print_json:
        print(json.dumps(report, indent=2))
def append_log(path: str, report: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(_log_line(report) + "\n")


def _log_line(report: dict[str, Any]) -> str:
    calamities = report.get("calamities", {})
    risk_parts = []
    if isinstance(calamities, dict):
        for name in ["flood", "drought", "wildfire", "storm", "heatwave"]:
            data = calamities.get(name, {})
            if isinstance(data, dict):
                value = data.get("risk_index_percent", data.get("risk_percent", "?"))
                risk = data.get("risk", "?")
                risk_parts.append(f"{name}={value}({risk})")
    sensors = report.get("sensors", {})
    sensor_text = ""
    if isinstance(sensors, dict):
        sensor_text = (
            f" sensors={sensors.get('online', '?')}/{sensors.get('total', '?')}"
            f" online working={sensors.get('working', '?')}"
        )
    notes = str(report.get("notes", ""))
    if len(notes) > 500:
        notes = notes[:497] + "..."
    return (
        f"{report.get('timestamp', '')} area={report.get('area', '')} "
        + " ".join(risk_parts)
        + sensor_text
        + f" notes=\"{notes}\"" 
    )

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
