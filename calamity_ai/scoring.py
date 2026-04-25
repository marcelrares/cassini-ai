from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .explanations import explain_score


# Risk labels are operational warning bands, not calibrated event probabilities.
RISK_LEVELS = [
    ("none", 0.00, 0.05),
    ("extremely_low", 0.05, 0.15),
    ("very_low", 0.15, 0.30),
    ("low", 0.30, 0.45),
    ("moderate", 0.45, 0.60),
    ("high", 0.60, 0.80),
    ("extreme", 0.80, 1.01),
]

ACTIVE_RISK_LEVELS = {"moderate", "high", "extreme"}

# Terrain runoff is a small modifier because elevation only approximates how fast
# rainfall can concentrate locally; observed/forecast precipitation stays dominant.
ROLLING_TERRAIN_RUNOFF = 0.12
STEEP_TERRAIN_RUNOFF = 0.22

FLOOD_FORECAST_WEIGHT = 0.65
FLOOD_RECENT_WETNESS_WEIGHT = 0.25
FLOOD_TERRAIN_RUNOFF_WEIGHT = 0.10

DROUGHT_PRECIP_WEIGHT = 0.15
DROUGHT_HEAT_WEIGHT = 0.20
DROUGHT_SOIL_WEIGHT = 0.25
DROUGHT_HISTORY_WEIGHT = 0.30
DROUGHT_EVAPOTRANSPIRATION_WEIGHT = 0.10

STORM_WIND_WEIGHT = 0.65
STORM_CAPE_WEIGHT = 0.35

WILDFIRE_HEAT_WEIGHT = 0.40
WILDFIRE_HUMIDITY_WEIGHT = 0.25
WILDFIRE_VPD_WEIGHT = 0.25
WILDFIRE_EVAPOTRANSPIRATION_WEIGHT = 0.10
WILDFIRE_FUEL_IGNITION_WEIGHT = 0.70
WILDFIRE_WIND_SPREAD_WEIGHT = 0.20
WILDFIRE_DROUGHT_HEAT_WEIGHT = 0.10

# Satellite data weight modifiers (satellite confidence gates overall impact)
SATELLITE_WEIGHT_SCALING = 0.25  # Max 25% weight from satellite data, rest from weather/context
FLOOD_SATELLITE_WEIGHT = 0.15  # Sentinel-1 water detection + CLMS moisture
DROUGHT_SATELLITE_WEIGHT = 0.12  # CLMS soil moisture anomaly + NDVI
WILDFIRE_SATELLITE_WEIGHT = 0.20  # FRP + burned area + NDVI
HEATWAVE_SATELLITE_WEIGHT = 0.10  # Sentinel-3 LST anomaly
STORM_SATELLITE_WEIGHT = 0.05  # Minimal satellite impact on storm

# Baseline anchors used when a metric starts becoming relevant for the index.
DROUGHT_HEAT_BASELINE_C = 20.0
HEATWAVE_BASELINE_C = 30.0
WILDFIRE_HEAT_BASELINE_C = 25.0
STORM_CAPE_HIGH_JKG = 2500.0
HUMIDITY_DRY_REFERENCE_PERCENT = 70.0
VPD_STRESS_REFERENCE_KPA = 1.6
SEASONAL_DEFICIT_FULL_SCALE = 0.8
DRY_DAYS_NORMAL_RATIO = 0.8
ET0_ANOMALY_FULL_SCALE = 0.5

SHORT_HISTORY_WEIGHT = 0.6
LONG_HISTORY_WEIGHT = 0.4
SHORT_WETNESS_WEIGHT = 0.65
LONG_WETNESS_WEIGHT = 0.35
SHORT_DRYNESS_WEIGHT = 0.40
LONG_DRYNESS_WEIGHT = 0.25
DRY_DAYS_WEIGHT = 0.20
LONG_DRY_DAYS_WEIGHT = 0.15


@dataclass(frozen=True)
class WeatherFeatures:
    precip_24h_m: float
    temp_mean_24h_c: float
    temp_max_24h_c: float
    wind_gust_max_ms: float
    cape_max_jkg: float
    temp_current_c: float | None = None
    temp_forecast_max_next_24h_c: float | None = None
    soil_moisture_proxy: float | None = None
    relative_humidity_mean_percent: float | None = None
    evapotranspiration_24h_mm: float | None = None
    vapor_pressure_deficit_kpa: float | None = None
    # Satellite-derived features (0-1 normalized indices)
    satellite_water_index: float | None = None  # Sentinel-1 SAR water mask (0=water, 1=dry)
    satellite_soil_moisture_anomaly: float | None = None  # CLMS SWI/SSM anomaly (-1=very wet, 0=normal, 1=very dry)
    satellite_fire_radiative_power: float | None = None  # Sentinel-3 FRP normalized (0=none, 1=max detected)
    satellite_burned_area_fraction: float | None = None  # MODIS MCD64A1 fraction (0=no burn, 1=fully burned)
    satellite_land_surface_temp_anomaly: float | None = None  # Sentinel-3 LST anomaly vs normal (-1=cold, 0=normal, 1=hot)
    satellite_ndvi_anomaly: float | None = None  # CLMS NDVI anomaly vs seasonal (-1=very low veg, 0=normal, 1=very high)
    satellite_optical_quality: float | None = None  # Sentinel-2 data quality (0=bad/cloudy, 1=excellent)
    satellite_radar_confidence: float | None = None  # Sentinel-1 data confidence (0=low, 1=high)


def clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


def label(score: float) -> str:
    for name, lower, upper in RISK_LEVELS:
        if lower <= score < upper:
            return name
    return "extreme"


def score_calamities(
    features: WeatherFeatures,
    thresholds: dict[str, float],
    context: Any | None = None,
) -> dict[str, dict[str, object]]:
    factors = _historical_factors(context)
    terrain_runoff = 0.0
    if factors["terrain_class"] == "rolling":
        terrain_runoff = ROLLING_TERRAIN_RUNOFF
    elif factors["terrain_class"] == "steep":
        terrain_runoff = STEEP_TERRAIN_RUNOFF

    # FLOOD SCORING: forecast rain + wetness + terrain + satellite water detection
    forecast_flood = clamp01(features.precip_24h_m / thresholds["flood_precip_high_m"])
    recent_wetness = factors["wetness_30d"]
    
    satellite_flood_boost = 0.0
    if features.satellite_water_index is not None and features.satellite_radar_confidence is not None:
        # Low water index + high confidence = detected water = flood risk boost
        water_detection = 1 - features.satellite_water_index  # Invert: 0=dry, 1=water
        satellite_flood_boost = water_detection * features.satellite_radar_confidence
    if features.satellite_soil_moisture_anomaly is not None:
        # Positive anomaly = wet soil = flood risk boost
        wet_signal = clamp01(features.satellite_soil_moisture_anomaly * -1)  # Invert for wetness
        satellite_flood_boost = max(satellite_flood_boost, wet_signal * 0.8)
    
    flood = clamp01(
        (FLOOD_FORECAST_WEIGHT * forecast_flood)
        + (FLOOD_RECENT_WETNESS_WEIGHT * recent_wetness)
        + (FLOOD_TERRAIN_RUNOFF_WEIGHT * terrain_runoff)
        + (FLOOD_SATELLITE_WEIGHT * satellite_flood_boost)
    )

    # DROUGHT SCORING: precip deficit + heat + soil dryness + history + satellite moisture
    drought_precip_factor = 1 - clamp01(features.precip_24h_m / thresholds["drought_precip_low_m"])
    drought_heat_factor = clamp01(
        (features.temp_mean_24h_c - DROUGHT_HEAT_BASELINE_C)
        / (thresholds["drought_temp_hot_c"] - DROUGHT_HEAT_BASELINE_C)
    )
    soil_dry_factor = 0.5
    if features.soil_moisture_proxy is not None:
        soil_dry_factor = 1 - clamp01(features.soil_moisture_proxy)
    
    satellite_drought_boost = 0.0
    if features.satellite_soil_moisture_anomaly is not None:
        # Positive anomaly = dry soil = drought risk
        satellite_drought_boost += clamp01(features.satellite_soil_moisture_anomaly) * 0.6
    if features.satellite_ndvi_anomaly is not None:
        # Negative anomaly = low vegetation = drought stress
        low_veg_stress = max(0.0, -features.satellite_ndvi_anomaly)
        satellite_drought_boost = max(satellite_drought_boost, low_veg_stress * 0.5)
    
    historical_dryness = factors["dryness_30d"]
    evapotranspiration_anomaly = factors["evapotranspiration"]
    drought = clamp01(
        (DROUGHT_PRECIP_WEIGHT * drought_precip_factor)
        + (DROUGHT_HEAT_WEIGHT * drought_heat_factor)
        + (DROUGHT_SOIL_WEIGHT * soil_dry_factor)
        + (DROUGHT_HISTORY_WEIGHT * historical_dryness)
        + (DROUGHT_EVAPOTRANSPIRATION_WEIGHT * evapotranspiration_anomaly)
        + (DROUGHT_SATELLITE_WEIGHT * satellite_drought_boost)
    )

    # STORM SCORING: wind + CAPE (minimal satellite impact)
    storm_wind = clamp01(features.wind_gust_max_ms / thresholds["storm_wind_high_ms"])
    storm_cape = clamp01(features.cape_max_jkg / STORM_CAPE_HIGH_JKG)
    storm = clamp01((STORM_WIND_WEIGHT * storm_wind) + (STORM_CAPE_WEIGHT * storm_cape))

    # HEATWAVE SCORING: temperature anomaly + satellite LST
    heatwave_base = clamp01(
        (features.temp_max_24h_c - HEATWAVE_BASELINE_C)
        / (thresholds["heat_high_c"] - HEATWAVE_BASELINE_C)
    )
    
    satellite_heat_boost = 0.0
    if features.satellite_land_surface_temp_anomaly is not None:
        # Positive anomaly = hot surface = heatwave boost
        satellite_heat_boost = clamp01(features.satellite_land_surface_temp_anomaly)
    
    heatwave = clamp01(heatwave_base + (HEATWAVE_SATELLITE_WEIGHT * satellite_heat_boost))

    # WILDFIRE SCORING: heat + wind + fuel dryness + satellite FRP + burned area + NDVI
    wildfire_heat = clamp01(
        (features.temp_max_24h_c - WILDFIRE_HEAT_BASELINE_C)
        / (thresholds["wildfire_temp_hot_c"] - WILDFIRE_HEAT_BASELINE_C)
    )
    wildfire_wind = clamp01(features.wind_gust_max_ms / thresholds["wildfire_wind_high_ms"])
    humidity_dryness = 0.5
    if features.relative_humidity_mean_percent is not None:
        humidity_dryness = 1 - clamp01(features.relative_humidity_mean_percent / HUMIDITY_DRY_REFERENCE_PERCENT)
    vpd_stress = 0.0
    if features.vapor_pressure_deficit_kpa is not None:
        vpd_stress = clamp01(features.vapor_pressure_deficit_kpa / VPD_STRESS_REFERENCE_KPA)
    fuel_dryness = max(historical_dryness, soil_dry_factor * 0.6)
    
    satellite_wildfire_boost = 0.0
    if features.satellite_fire_radiative_power is not None:
        # Direct fire signal from FRP = strong wildfire risk boost
        satellite_wildfire_boost += features.satellite_fire_radiative_power * 0.8
    if features.satellite_burned_area_fraction is not None:
        # Recent burns = residual fire risk
        satellite_wildfire_boost = max(satellite_wildfire_boost, features.satellite_burned_area_fraction * 0.6)
    if features.satellite_ndvi_anomaly is not None:
        # Low vegetation + high temperature = higher wildfire spread potential
        low_veg_stress = max(0.0, -features.satellite_ndvi_anomaly)
        satellite_wildfire_boost += low_veg_stress * 0.4
    
    ignition_weather = clamp01(
        (WILDFIRE_HEAT_WEIGHT * wildfire_heat)
        + (WILDFIRE_HUMIDITY_WEIGHT * humidity_dryness)
        + (WILDFIRE_VPD_WEIGHT * vpd_stress)
        + (WILDFIRE_EVAPOTRANSPIRATION_WEIGHT * evapotranspiration_anomaly)
    )
    
    wildfire = clamp01(
        (WILDFIRE_FUEL_IGNITION_WEIGHT * fuel_dryness * ignition_weather)
        + (WILDFIRE_WIND_SPREAD_WEIGHT * wildfire_wind * fuel_dryness)
        + (WILDFIRE_DROUGHT_HEAT_WEIGHT * drought * wildfire_heat)
        + (WILDFIRE_SATELLITE_WEIGHT * satellite_wildfire_boost)
    )

    raw_scores = {
        "flood": flood,
        "drought": drought,
        "storm": storm,
        "heatwave": heatwave,
        "wildfire": wildfire,
    }
    return {
        name: explain_score(name, score, label(score), features, thresholds, factors)
        for name, score in raw_scores.items()
    }


def _historical_factors(context: Any | None) -> dict[str, Any]:
    if context is None:
        return {
            "dryness_30d": 0.5,
            "wetness_30d": 0.0,
            "evapotranspiration": 0.0,
            "terrain_class": "unknown",
            "baseline_available": False,
            "precipitation_30d_ratio": None,
            "precipitation_90d_ratio": None,
            "dry_days_ratio": None,
            "dry_days_90d_ratio": None,
            "evapotranspiration_ratio": None,
            "evapotranspiration_90d_ratio": None,
        }
    baseline = getattr(getattr(context, "history", None), "seasonal_baseline", None)
    precipitation_ratio = getattr(baseline, "precipitation_30d_ratio", None)
    precipitation_90d_ratio = getattr(baseline, "precipitation_90d_ratio", None)
    dry_days_ratio = getattr(baseline, "dry_days_ratio", None)
    dry_days_90d_ratio = getattr(baseline, "dry_days_90d_ratio", None)
    et0_ratio = getattr(baseline, "evapotranspiration_ratio", None)
    et0_90d_ratio = getattr(baseline, "evapotranspiration_90d_ratio", None)

    rainfall_deficit = 0.5
    wetness = 0.0
    if precipitation_ratio is not None:
        rainfall_deficit = clamp01((1.0 - precipitation_ratio) / SEASONAL_DEFICIT_FULL_SCALE)
        wetness = clamp01((precipitation_ratio - 1.0) / 1.0)
    long_rainfall_deficit = 0.5
    long_wetness = 0.0
    if precipitation_90d_ratio is not None:
        long_rainfall_deficit = clamp01((1.0 - precipitation_90d_ratio) / SEASONAL_DEFICIT_FULL_SCALE)
        long_wetness = clamp01((precipitation_90d_ratio - 1.0) / 1.0)
    dry_days_anomaly = (
        0.5
        if dry_days_ratio is None
        else clamp01((dry_days_ratio - DRY_DAYS_NORMAL_RATIO) / DRY_DAYS_NORMAL_RATIO)
    )
    dry_days_90d_anomaly = (
        0.5
        if dry_days_90d_ratio is None
        else clamp01((dry_days_90d_ratio - DRY_DAYS_NORMAL_RATIO) / DRY_DAYS_NORMAL_RATIO)
    )
    et0_anomaly_30d = 0.0 if et0_ratio is None else clamp01((et0_ratio - 1.0) / ET0_ANOMALY_FULL_SCALE)
    et0_anomaly_90d = 0.0 if et0_90d_ratio is None else clamp01((et0_90d_ratio - 1.0) / ET0_ANOMALY_FULL_SCALE)
    # Historical dryness combines short- and long-window rainfall deficit,
    # dry-day frequency, and evapotranspiration stress against seasonal normals.
    et0_anomaly = clamp01((SHORT_HISTORY_WEIGHT * et0_anomaly_30d) + (LONG_HISTORY_WEIGHT * et0_anomaly_90d))
    dryness = clamp01(
        (SHORT_DRYNESS_WEIGHT * rainfall_deficit)
        + (LONG_DRYNESS_WEIGHT * long_rainfall_deficit)
        + (DRY_DAYS_WEIGHT * dry_days_anomaly)
        + (LONG_DRY_DAYS_WEIGHT * dry_days_90d_anomaly)
    )
    elevation = getattr(context, "elevation", None)
    return {
        "dryness_30d": dryness,
        "wetness_30d": clamp01((SHORT_WETNESS_WEIGHT * wetness) + (LONG_WETNESS_WEIGHT * long_wetness)),
        "evapotranspiration": et0_anomaly,
        "terrain_class": getattr(elevation, "terrain_class", "unknown"),
        "baseline_available": bool(getattr(baseline, "baseline_years", [])),
        "precipitation_30d_ratio": precipitation_ratio,
        "precipitation_90d_ratio": precipitation_90d_ratio,
        "dry_days_ratio": dry_days_ratio,
        "dry_days_90d_ratio": dry_days_90d_ratio,
        "evapotranspiration_ratio": et0_ratio,
        "evapotranspiration_90d_ratio": et0_90d_ratio,
    }
