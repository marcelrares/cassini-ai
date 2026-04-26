from __future__ import annotations

from dataclasses import dataclass

from .config import MonitorConfig
from .geo import bbox, centroid


@dataclass(frozen=True)
class RegionalCalibration:
    region: str
    confidence: str
    thresholds: dict[str, float]
    notes: str


def calibrate_thresholds(config: MonitorConfig, thresholds: dict[str, float]) -> RegionalCalibration:
    center_lon, center_lat = centroid(config.polygon)
    west, south, east, north = bbox(config.polygon)
    region = _region(center_lon, center_lat)
    calibrated = dict(thresholds)
    confidence = "regional_heuristic"
    notes = "Thresholds adjusted from bbox location and broad climate region; calibrate with local observed events for production."

    if region == "mediterranean":
        calibrated["drought_temp_hot_c"] = max(calibrated["drought_temp_hot_c"], 32.0)
        calibrated["heat_high_c"] = max(calibrated["heat_high_c"], 40.0)
        calibrated["wildfire_temp_hot_c"] = max(calibrated["wildfire_temp_hot_c"], 34.0)
        calibrated["wildfire_wind_high_ms"] = max(calibrated["wildfire_wind_high_ms"], 13.0)
        calibrated["drought_precip_low_m"] = min(calibrated["drought_precip_low_m"], 0.003)
    elif region == "continental_europe":
        calibrated["heat_high_c"] = min(max(calibrated["heat_high_c"], 36.0), 38.0)
        calibrated["wildfire_temp_hot_c"] = min(max(calibrated["wildfire_temp_hot_c"], 31.0), 33.0)
    elif region == "northern_europe":
        calibrated["heat_high_c"] = min(calibrated["heat_high_c"], 34.0)
        calibrated["drought_temp_hot_c"] = min(calibrated["drought_temp_hot_c"], 28.0)
        calibrated["wildfire_temp_hot_c"] = min(calibrated["wildfire_temp_hot_c"], 29.0)
    elif region == "middle_east_dry":
        calibrated["drought_temp_hot_c"] = max(calibrated["drought_temp_hot_c"], 34.0)
        calibrated["heat_high_c"] = max(calibrated["heat_high_c"], 42.0)
        calibrated["wildfire_temp_hot_c"] = max(calibrated["wildfire_temp_hot_c"], 36.0)
        calibrated["drought_precip_low_m"] = min(calibrated["drought_precip_low_m"], 0.002)
    elif region == "tropical":
        calibrated["flood_precip_high_m"] = max(calibrated["flood_precip_high_m"], 0.08)
        calibrated["heat_high_c"] = max(calibrated["heat_high_c"], 38.0)

    area_degrees = abs(east - west) * abs(north - south)
    if area_degrees > 25.0:
        confidence = "low_large_area"
        notes += " Bbox is large; use tiled scoring for more precise regional hazards."

    return RegionalCalibration(
        region=region,
        confidence=confidence,
        thresholds=calibrated,
        notes=notes,
    )


def _region(lon: float, lat: float) -> str:
    if 35.0 <= lat <= 72.0 and -25.0 <= lon <= 45.0:
        if lat >= 55.0:
            return "northern_europe"
        if lat <= 45.0 or (25.0 <= lon <= 45.0 and lat <= 43.0):
            return "mediterranean"
        return "continental_europe"
    if 25.0 <= lat <= 43.0 and 25.0 <= lon <= 60.0:
        return "middle_east_dry"
    if -23.5 <= lat <= 23.5:
        return "tropical"
    if abs(lat) >= 55.0:
        return "cold_high_latitude"
    return "global_default"
