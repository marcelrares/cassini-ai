from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


def append_jsonl(path: str | Path, report: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(report, sort_keys=True) + "\n")


def append_csv(path: str | Path, report: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    row = flatten_report(report)
    write_header = not target.exists() or target.stat().st_size == 0
    with target.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def flatten_report(report: dict[str, Any]) -> dict[str, Any]:
    row: dict[str, Any] = {
        "timestamp": report.get("timestamp"),
        "area": report.get("area"),
        "working": report.get("working"),
        "notes": report.get("notes"),
    }
    _add_weather_columns(row, report.get("weather"))
    _add_calamity_columns(row, report.get("calamities"))
    _add_sensor_columns(row, report.get("sensors"))
    return row


def _add_weather_columns(row: dict[str, Any], weather: object) -> None:
    if not isinstance(weather, dict):
        return
    for key in [
        "precip_24h_m",
        "temp_mean_24h_c",
        "temp_max_24h_c",
        "wind_gust_max_ms",
        "cape_max_jkg",
        "soil_moisture_proxy",
        "relative_humidity_mean_percent",
        "evapotranspiration_24h_mm",
        "vapor_pressure_deficit_kpa",
    ]:
        row[f"weather_{key}"] = weather.get(key)


def _add_calamity_columns(row: dict[str, Any], calamities: object) -> None:
    if not isinstance(calamities, dict):
        return
    for name in ["flood", "drought", "wildfire", "storm", "heatwave"]:
        data = calamities.get(name)
        if not isinstance(data, dict):
            continue
        row[f"{name}_score"] = data.get("score")
        row[f"{name}_risk_index_percent"] = data.get("risk_index_percent", data.get("risk_percent"))
        row[f"{name}_risk"] = data.get("risk")


def _add_sensor_columns(row: dict[str, Any], sensors: object) -> None:
    if not isinstance(sensors, dict):
        return
    for key in ["total", "online", "offline", "stale", "working"]:
        row[f"sensors_{key}"] = sensors.get(key)
