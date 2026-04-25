from __future__ import annotations

import argparse
import json
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
    get_earth_engine_weather_features,
    get_local_weather_features,
    get_open_meteo_weather_features,
)
from calamity_ai.zones import get_zone_analysis, zone_analysis_to_dict


def build_report(args: argparse.Namespace) -> dict[str, object]:
    config = load_runtime_config(args)
    now = datetime.now(timezone.utc)
    resource_summary = None
    if not args.no_resources:
        try:
            resource_summary = ensure_resources(config, update=args.update_resources)
        except Exception as exc:
            resource_summary = {
                "error": f"Resource cache could not be loaded or refreshed: {exc}",
                "explanation": "The monitor continued without OSM boundary/hydrology/land-cover resource refinement.",
            }

    if args.demo:
        features = demo_weather_features()
    elif args.provider == "local":
        features = get_local_weather_features(args.weather)
    elif args.provider == "openmeteo":
        features = get_open_meteo_weather_features(config)
    else:
        features = get_earth_engine_weather_features(config)

    sensors = summarize_sensors(
        args.sensors,
        now=now,
        min_online_ratio=config.sensor_health["min_online_ratio"],
        stale_after_minutes=1e9 if args.demo else config.sensor_health["stale_after_minutes"],
    )
    context = None
    if not args.no_context:
        context = get_environmental_context(config, now=now)
    calamities = score_calamities(features, config.thresholds, context=context)
    zone_analysis = None
    if context and not args.no_zones:
        zone_analysis = get_zone_analysis(
            config,
            features=features,
            calamities=calamities,
            context=context,
        )
    predictions = None
    if context and not args.no_predictions and args.provider == "openmeteo":
        predictions = get_open_meteo_predictions(config, context=context, days=args.prediction_days)
    copernicus = None
    if not args.no_copernicus:
        copernicus = get_copernicus_summary(config, now=now)

    notes = []
    if sensors.offline:
        notes.append(f"{sensors.offline} sensors offline or stale")
    elevated = [name for name, risk in calamities.items() if risk["risk"] in {"medium", "high"}]
    if elevated:
        notes.append("elevated risks: " + ", ".join(elevated))
    if copernicus and not copernicus.flood_observation_ready:
        notes.append("no recent Sentinel-1 products found for flood validation")
    if context:
        notes.append(context.history.explanation)
        notes.append(context.history.seasonal_baseline.explanation)
        notes.append(context.elevation.explanation)
    if zone_analysis:
        top_flood = ", ".join(zone.name for zone in zone_analysis.top_flood_zones)
        notes.append(f"Highest relative flood-exposure sectors, not probabilities: {top_flood}")
    if predictions and predictions.daily:
        notes.append("Prediction summary: " + predictions.daily[0].summary)

    report = {
        "timestamp": now.isoformat().replace("+00:00", "Z"),
        "area": config.area_name,
        "weather": features_to_dict(features),
        "calamities": calamities,
        "sensors": {
            "total": sensors.total,
            "online": sensors.online,
            "offline": sensors.offline,
            "stale": sensors.stale,
            "working": sensors.working,
        },
        "working": sensors.working,
        "notes": "; ".join(notes) if notes else "all monitored systems nominal",
    }
    if copernicus:
        report["copernicus"] = copernicus_to_dict(copernicus)
    if context:
        report["context"] = environmental_context_to_dict(context)
    if zone_analysis:
        report["zone_analysis"] = zone_analysis_to_dict(zone_analysis)
    if predictions:
        report["predictions"] = predictions_to_dict(predictions)
    if resource_summary:
        report["resources"] = (
            resource_summary if isinstance(resource_summary, dict) else resource_summary_to_dict(resource_summary)
        )
    return report


def load_runtime_config(args: argparse.Namespace) -> object:
    config = load_config(args.config).with_project_id(args.project or os.getenv("EE_PROJECT_ID"))
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
    parser.add_argument("--provider", choices=["openmeteo", "local", "earthengine"], default="openmeteo")
    parser.add_argument("--no-copernicus", action="store_true", help="Skip Copernicus satellite catalogue checks")
    parser.add_argument("--no-context", action="store_true", help="Skip historical weather and elevation context")
    parser.add_argument("--no-zones", action="store_true", help="Skip local zone exposure ranking")
    parser.add_argument("--no-predictions", action="store_true", help="Skip multi-day forecast predictions")
    parser.add_argument("--no-resources", action="store_true", help="Skip OSM resource cache loading")
    parser.add_argument("--update-resources", action="store_true", help="Refresh cached OSM boundary and context resources")
    parser.add_argument("--prediction-days", type=int, default=5)
    parser.add_argument("--project", help="Google Cloud project id for Earth Engine")
    parser.add_argument("--save-project", action="store_true", help="Save --project into the config file")
    parser.add_argument("--demo", action="store_true", help="Run without Google Earth Engine")
    parser.add_argument("--print-json", action="store_true", help="Print the full JSON report to console")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.project and args.save_project:
        config_path = Path(args.config)
        raw = json.loads(config_path.read_text(encoding="utf-8"))
        raw["project_id"] = args.project
        config_path.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")
    report = build_report(args)
    append_jsonl(args.jsonl_out, report)
    append_csv(args.csv_out, report)
    append_log(args.log_out, report)
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
