from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class WeatherFeatures:
    precip_24h_m: float
    temp_mean_24h_c: float
    temp_max_24h_c: float
    wind_gust_max_ms: float
    cape_max_jkg: float
    soil_moisture_proxy: float | None = None
    relative_humidity_mean_percent: float | None = None
    evapotranspiration_24h_mm: float | None = None
    vapor_pressure_deficit_kpa: float | None = None


def clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


def label(score: float) -> str:
    if score >= 0.7:
        return "high"
    if score >= 0.4:
        return "medium"
    return "low"


def score_calamities(
    features: WeatherFeatures,
    thresholds: dict[str, float],
    context: Any | None = None,
) -> dict[str, dict[str, object]]:
    factors = _historical_factors(context)
    terrain_runoff = 0.0
    if factors["terrain_class"] == "rolling":
        terrain_runoff = 0.12
    elif factors["terrain_class"] == "steep":
        terrain_runoff = 0.22

    forecast_flood = clamp01(features.precip_24h_m / thresholds["flood_precip_high_m"])
    recent_wetness = factors["wetness_30d"]
    flood = clamp01((0.65 * forecast_flood) + (0.25 * recent_wetness) + (0.10 * terrain_runoff))

    drought_precip_factor = 1 - clamp01(features.precip_24h_m / thresholds["drought_precip_low_m"])
    drought_heat_factor = clamp01((features.temp_mean_24h_c - 20) / (thresholds["drought_temp_hot_c"] - 20))
    soil_dry_factor = 0.5
    if features.soil_moisture_proxy is not None:
        soil_dry_factor = 1 - clamp01(features.soil_moisture_proxy)
    historical_dryness = factors["dryness_30d"]
    evapotranspiration_anomaly = factors["evapotranspiration"]
    drought = clamp01(
        (0.15 * drought_precip_factor)
        + (0.20 * drought_heat_factor)
        + (0.25 * soil_dry_factor)
        + (0.30 * historical_dryness)
        + (0.10 * evapotranspiration_anomaly)
    )

    storm_wind = clamp01(features.wind_gust_max_ms / thresholds["storm_wind_high_ms"])
    storm_cape = clamp01(features.cape_max_jkg / 2500)
    storm = clamp01((0.65 * storm_wind) + (0.35 * storm_cape))

    heatwave = clamp01((features.temp_max_24h_c - 30) / (thresholds["heat_high_c"] - 30))

    wildfire_heat = clamp01((features.temp_max_24h_c - 25) / (thresholds["wildfire_temp_hot_c"] - 25))
    wildfire_wind = clamp01(features.wind_gust_max_ms / thresholds["wildfire_wind_high_ms"])
    humidity_dryness = 0.5
    if features.relative_humidity_mean_percent is not None:
        humidity_dryness = 1 - clamp01(features.relative_humidity_mean_percent / 70)
    vpd_stress = 0.0
    if features.vapor_pressure_deficit_kpa is not None:
        vpd_stress = clamp01(features.vapor_pressure_deficit_kpa / 1.6)
    fuel_dryness = max(historical_dryness, soil_dry_factor * 0.6)
    ignition_weather = clamp01(
        (0.40 * wildfire_heat)
        + (0.25 * humidity_dryness)
        + (0.25 * vpd_stress)
        + (0.10 * evapotranspiration_anomaly)
    )
    wildfire = clamp01(
        (0.70 * fuel_dryness * ignition_weather)
        + (0.20 * wildfire_wind * fuel_dryness)
        + (0.10 * drought * wildfire_heat)
    )

    raw_scores = {
        "flood": flood,
        "drought": drought,
        "storm": storm,
        "heatwave": heatwave,
        "wildfire": wildfire,
    }
    return {
        name: _explain_score(name, score, features, thresholds, factors)
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
        rainfall_deficit = clamp01((1.0 - precipitation_ratio) / 0.8)
        wetness = clamp01((precipitation_ratio - 1.0) / 1.0)
    long_rainfall_deficit = 0.5
    long_wetness = 0.0
    if precipitation_90d_ratio is not None:
        long_rainfall_deficit = clamp01((1.0 - precipitation_90d_ratio) / 0.8)
        long_wetness = clamp01((precipitation_90d_ratio - 1.0) / 1.0)
    dry_days_anomaly = 0.5 if dry_days_ratio is None else clamp01((dry_days_ratio - 0.8) / 0.8)
    dry_days_90d_anomaly = 0.5 if dry_days_90d_ratio is None else clamp01((dry_days_90d_ratio - 0.8) / 0.8)
    et0_anomaly_30d = 0.0 if et0_ratio is None else clamp01((et0_ratio - 1.0) / 0.5)
    et0_anomaly_90d = 0.0 if et0_90d_ratio is None else clamp01((et0_90d_ratio - 1.0) / 0.5)
    et0_anomaly = clamp01((0.6 * et0_anomaly_30d) + (0.4 * et0_anomaly_90d))
    dryness = clamp01(
        (0.40 * rainfall_deficit)
        + (0.25 * long_rainfall_deficit)
        + (0.20 * dry_days_anomaly)
        + (0.15 * dry_days_90d_anomaly)
    )
    elevation = getattr(context, "elevation", None)
    return {
        "dryness_30d": dryness,
        "wetness_30d": clamp01((0.65 * wetness) + (0.35 * long_wetness)),
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


def _explain_score(
    name: str,
    score: float,
    features: WeatherFeatures,
    thresholds: dict[str, float],
    factors: dict[str, Any],
) -> dict[str, object]:
    risk = label(score)
    percent = round(score * 100, 1)
    explanations = {
        "flood": _flood_explanation(score, risk, features, thresholds, factors),
        "drought": _drought_explanation(score, risk, features, thresholds, factors),
        "storm": _storm_explanation(score, risk, features, thresholds),
        "heatwave": _heatwave_explanation(score, risk, features, thresholds),
        "wildfire": _wildfire_explanation(score, risk, features, thresholds, factors),
    }
    return {
        "score": round(score, 3),
        "risk_percent": percent,
        "risk_index_percent": percent,
        "is_probability": False,
        "risk": risk,
        **explanations[name],
    }


def _risk_scale() -> dict[str, str]:
    return {
        "low": "0-39.9%: low risk",
        "medium": "40-69.9%: medium risk",
        "high": "70-100%: high risk",
    }


def _mm(meters: float) -> float:
    return round(meters * 1000, 1)


def _c(value: float) -> float:
    return round(value, 1)


def _ms(value: float) -> float:
    return round(value, 1)


def _flood_explanation(
    score: float,
    risk: str,
    features: WeatherFeatures,
    thresholds: dict[str, float],
    factors: dict[str, Any],
) -> dict[str, object]:
    precip_mm = _mm(features.precip_24h_m)
    high_mm = _mm(thresholds["flood_precip_high_m"])
    return {
        "title": "Flood",
        "explanation": (
            f"Flood risk is {round(score * 100, 1)}% ({risk}) because the forecast shows "
            f"{precip_mm} mm of precipitation in the next 24 hours, then adjusts that signal using recent wetness versus "
            f"same-season historical rainfall and terrain. The high-risk reference threshold is {high_mm} mm/24h; "
            f"values below {round(high_mm * 0.4, 1)} mm/24h are normally treated as low risk."
        ),
        "input_values": {
            "precipitation_24h_mm": precip_mm,
            "historical_wetness_factor": round(float(factors["wetness_30d"]), 3),
            "terrain_class": factors["terrain_class"],
            "precipitation_30d_ratio_vs_baseline": factors["precipitation_30d_ratio"],
            "precipitation_90d_ratio_vs_baseline": factors["precipitation_90d_ratio"],
        },
        "normal_limits": {
            "low": f"below {round(high_mm * 0.4, 1)} mm/24h",
            "medium": f"{round(high_mm * 0.4, 1)}-{round(high_mm * 0.7, 1)} mm/24h",
            "high": f"above {round(high_mm * 0.7, 1)} mm/24h, critical reference at {high_mm} mm/24h",
        },
        "formula": "65% forecast precipitation + 25% recent wetness versus historical baseline + 10% terrain runoff factor",
        "risk_scale": _risk_scale(),
    }


def _drought_explanation(
    score: float,
    risk: str,
    features: WeatherFeatures,
    thresholds: dict[str, float],
    factors: dict[str, Any],
) -> dict[str, object]:
    precip_mm = _mm(features.precip_24h_m)
    low_precip_mm = _mm(thresholds["drought_precip_low_m"])
    soil_text = "unknown"
    if features.soil_moisture_proxy is not None:
        soil_text = f"{round(features.soil_moisture_proxy * 100, 1)}% of the wet reference"
    factor_notes = [
        _factor_note(
            "precipitation",
            drought_precip_factor := 1 - clamp01(features.precip_24h_m / thresholds["drought_precip_low_m"]),
        ),
        _factor_note(
            "temperature",
            clamp01((features.temp_mean_24h_c - 20) / (thresholds["drought_temp_hot_c"] - 20)),
        ),
    ]
    if features.soil_moisture_proxy is not None:
        factor_notes.append(_factor_note("dry soil", 1 - clamp01(features.soil_moisture_proxy)))
    factor_notes.append(_factor_note("30-day historical dryness", float(factors["dryness_30d"])))
    return {
        "title": "Drought",
        "explanation": (
            f"Drought risk is {round(score * 100, 1)}% ({risk}) because expected precipitation is "
            f"{precip_mm} mm/24h, mean temperature is {_c(features.temp_mean_24h_c)} C, and soil moisture is "
            f"{soil_text}. For drought, precipitation below {low_precip_mm} mm/24h and temperatures above "
            f"{_c(thresholds['drought_temp_hot_c'])} C increase risk. The score now uses previous years for the same seasonal window, "
            f"so one dry forecast day is not enough by itself to create high drought risk. In this calculation: {', '.join(factor_notes)}."
        ),
        "input_values": {
            "precipitation_24h_mm": precip_mm,
            "temperature_mean_24h_c": _c(features.temp_mean_24h_c),
            "soil_moisture_proxy_percent": None
            if features.soil_moisture_proxy is None
            else round(features.soil_moisture_proxy * 100, 1),
            "historical_dryness_factor": round(float(factors["dryness_30d"]), 3),
            "precipitation_30d_ratio_vs_baseline": factors["precipitation_30d_ratio"],
            "dry_days_ratio_vs_baseline": factors["dry_days_ratio"],
            "evapotranspiration_ratio_vs_baseline": factors["evapotranspiration_ratio"],
            "precipitation_90d_ratio_vs_baseline": factors["precipitation_90d_ratio"],
            "dry_days_90d_ratio_vs_baseline": factors["dry_days_90d_ratio"],
            "evapotranspiration_90d_ratio_vs_baseline": factors["evapotranspiration_90d_ratio"],
        },
        "normal_limits": {
            "low": f"above {low_precip_mm} mm/24h or sufficiently moist soil",
            "medium": f"very low precipitation and temperature rising toward {_c(thresholds['drought_temp_hot_c'])} C",
            "high": f"near-zero precipitation, dry soil, and temperature above {_c(thresholds['drought_temp_hot_c'])} C",
        },
        "formula": "15% next-24h precipitation deficit + 20% heat + 25% soil dryness + 30% historical 30-day dryness + 10% evapotranspiration anomaly",
        "risk_scale": _risk_scale(),
    }


def _factor_note(name: str, factor: float) -> str:
    if factor >= 0.7:
        level = "strongly increases risk"
    elif factor >= 0.4:
        level = "moderately increases risk"
    else:
        level = "reduces risk"
    return f"{name} {level}"


def _storm_explanation(
    score: float,
    risk: str,
    features: WeatherFeatures,
    thresholds: dict[str, float],
) -> dict[str, object]:
    high_wind = thresholds["storm_wind_high_ms"]
    return {
        "title": "Storm / strong wind",
        "explanation": (
            f"Storm risk is {round(score * 100, 1)}% ({risk}) because maximum forecast gusts are "
            f"{_ms(features.wind_gust_max_ms)} m/s and CAPE is {_c(features.cape_max_jkg)} J/kg. "
            f"The high-risk wind reference is {_ms(high_wind)} m/s; CAPE above 2500 J/kg indicates strong convective instability."
        ),
        "input_values": {
            "wind_gust_max_ms": _ms(features.wind_gust_max_ms),
            "cape_max_jkg": _c(features.cape_max_jkg),
        },
        "normal_limits": {
            "low": f"gusts below {_ms(high_wind * 0.4)} m/s and low CAPE",
            "medium": f"gusts between {_ms(high_wind * 0.4)} and {_ms(high_wind * 0.7)} m/s or moderate CAPE",
            "high": f"gusts above {_ms(high_wind * 0.7)} m/s, critical reference {_ms(high_wind)} m/s, or very high CAPE",
        },
        "formula": "65% wind gusts + 35% CAPE",
        "risk_scale": _risk_scale(),
    }


def _heatwave_explanation(
    score: float,
    risk: str,
    features: WeatherFeatures,
    thresholds: dict[str, float],
) -> dict[str, object]:
    return {
        "title": "Heatwave",
        "explanation": (
            f"Heatwave risk is {round(score * 100, 1)}% ({risk}) because the forecast maximum temperature is "
            f"{_c(features.temp_max_24h_c)} C. Risk starts increasing above 30 C, and the high-risk reference threshold is "
            f"{_c(thresholds['heat_high_c'])} C."
        ),
        "input_values": {
            "temperature_max_24h_c": _c(features.temp_max_24h_c),
        },
        "normal_limits": {
            "low": "below 30 C",
            "medium": f"approximately 30-{_c(thresholds['heat_high_c'] * 0.92)} C",
            "high": f"near/above {_c(thresholds['heat_high_c'])} C",
        },
        "formula": "(maximum_temperature_24h - 30 C) / (heatwave_threshold - 30 C)",
        "risk_scale": _risk_scale(),
    }


def _wildfire_explanation(
    score: float,
    risk: str,
    features: WeatherFeatures,
    thresholds: dict[str, float],
    factors: dict[str, Any],
) -> dict[str, object]:
    return {
        "title": "Wildfire",
        "explanation": (
            f"Spontaneous wildfire risk index is {round(score * 100, 1)}% ({risk}). This is not a probability; it is a normalized warning index. "
            f"The model now requires dry fuel conditions plus ignition weather, then uses wind mainly as a spread amplifier. It combines historical dryness, "
            f"maximum temperature {_c(features.temp_max_24h_c)} C, and wind gusts {_ms(features.wind_gust_max_ms)} m/s. "
            f"Risk rises sharply above {_c(thresholds['wildfire_temp_hot_c'])} C and above "
            f"{_ms(thresholds['wildfire_wind_high_ms'])} m/s. Historical dryness is compared with previous years for the same season, "
            "so wind alone or one dry forecast day should not produce an excessive spontaneous-fire score."
        ),
        "input_values": {
            "temperature_max_24h_c": _c(features.temp_max_24h_c),
            "wind_gust_max_ms": _ms(features.wind_gust_max_ms),
            "soil_moisture_proxy_percent": None
            if features.soil_moisture_proxy is None
            else round(features.soil_moisture_proxy * 100, 1),
            "relative_humidity_mean_percent": None
            if features.relative_humidity_mean_percent is None
            else round(features.relative_humidity_mean_percent, 1),
            "evapotranspiration_24h_mm": None
            if features.evapotranspiration_24h_mm is None
            else round(features.evapotranspiration_24h_mm, 1),
            "vapor_pressure_deficit_kpa": None
            if features.vapor_pressure_deficit_kpa is None
            else round(features.vapor_pressure_deficit_kpa, 2),
            "historical_dryness_factor": round(float(factors["dryness_30d"]), 3),
            "precipitation_30d_ratio_vs_baseline": factors["precipitation_30d_ratio"],
            "precipitation_90d_ratio_vs_baseline": factors["precipitation_90d_ratio"],
        },
        "normal_limits": {
            "low": f"temperatures below 25 C, weak wind, and moist soil",
            "medium": f"rising temperature, wind near {_ms(thresholds['wildfire_wind_high_ms'])} m/s, or drought signs",
            "high": f"temperature above {_c(thresholds['wildfire_temp_hot_c'])} C, wind above {_ms(thresholds['wildfire_wind_high_ms'])} m/s, and dry soil",
        },
        "formula": "70% fuel dryness multiplied by ignition weather + 20% wind spread factor gated by fuel dryness + 10% drought-and-heat interaction",
        "risk_scale": _risk_scale(),
    }
