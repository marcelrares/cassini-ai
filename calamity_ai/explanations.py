from __future__ import annotations

from typing import Any


def explain_score(
    name: str,
    score: float,
    risk: str,
    features: Any,
    thresholds: dict[str, float],
    factors: dict[str, Any],
) -> dict[str, object]:
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
    features: Any,
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
        "risk_scale": _risk_scale(),
    }


def _drought_explanation(
    score: float,
    risk: str,
    features: Any,
    thresholds: dict[str, float],
    factors: dict[str, Any],
) -> dict[str, object]:
    precip_mm = _mm(features.precip_24h_m)
    low_precip_mm = _mm(thresholds["drought_precip_low_m"])
    soil_text = "unknown"
    if features.soil_moisture_proxy is not None:
        soil_text = f"{round(features.soil_moisture_proxy * 100, 1)}% of the wet reference"
    current_temp_text = ""
    if getattr(features, "temp_current_c", None) is not None:
        current_temp_text = f" Current temperature is {_c(features.temp_current_c)} C."
    factor_notes = [
        _factor_note(
            "precipitation",
            1 - _clamp01(features.precip_24h_m / thresholds["drought_precip_low_m"]),
        ),
        _factor_note(
            "temperature",
            _clamp01((features.temp_mean_24h_c - 20) / (thresholds["drought_temp_hot_c"] - 20)),
        ),
    ]
    if features.soil_moisture_proxy is not None:
        factor_notes.append(_factor_note("dry soil", 1 - _clamp01(features.soil_moisture_proxy)))
    factor_notes.append(_factor_note("30-day historical dryness", float(factors["dryness_30d"])))
    return {
        "title": "Drought",
        "explanation": (
            f"Drought risk is {round(score * 100, 1)}% ({risk}) because expected precipitation is "
            f"{precip_mm} mm/24h, observed mean temperature over the last 24 hours is {_c(features.temp_mean_24h_c)} C, and soil moisture is "
            f"{soil_text}. For drought, precipitation below {low_precip_mm} mm/24h and temperatures above "
            f"{_c(thresholds['drought_temp_hot_c'])} C increase risk. The score now uses previous years for the same seasonal window, "
            f"so one dry forecast day is not enough by itself to create high drought risk.{current_temp_text} "
            f"In this calculation: {', '.join(factor_notes)}."
        ),
        "input_values": {
            "precipitation_24h_mm": precip_mm,
            "temperature_mean_24h_c": _c(features.temp_mean_24h_c),
            "temperature_current_c": None
            if getattr(features, "temp_current_c", None) is None
            else _c(features.temp_current_c),
            "temperature_forecast_max_next_24h_c": None
            if getattr(features, "temp_forecast_max_next_24h_c", None) is None
            else _c(features.temp_forecast_max_next_24h_c),
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
    features: Any,
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
        "risk_scale": _risk_scale(),
    }


def _heatwave_explanation(
    score: float,
    risk: str,
    features: Any,
    thresholds: dict[str, float],
) -> dict[str, object]:
    forecast_max = getattr(features, "temp_forecast_max_next_24h_c", None)
    forecast_text = ""
    if forecast_max is not None:
        forecast_text = f" The forecast maximum for the next 24 hours is {_c(forecast_max)} C."
    return {
        "title": "Heatwave",
        "explanation": (
            f"Heatwave risk is {round(score * 100, 1)}% ({risk}) because the observed maximum temperature over the last 24 hours is "
            f"{_c(features.temp_max_24h_c)} C.{forecast_text} Risk starts increasing above 30 C, and the high-risk reference threshold is "
            f"{_c(thresholds['heat_high_c'])} C."
        ),
        "input_values": {
            "temperature_max_24h_c": _c(features.temp_max_24h_c),
            "temperature_current_c": None
            if getattr(features, "temp_current_c", None) is None
            else _c(features.temp_current_c),
            "temperature_forecast_max_next_24h_c": None
            if forecast_max is None
            else _c(forecast_max),
        },
        "normal_limits": {
            "low": "below 30 C",
            "medium": f"approximately 30-{_c(thresholds['heat_high_c'] * 0.92)} C",
            "high": f"near/above {_c(thresholds['heat_high_c'])} C",
        },
        "risk_scale": _risk_scale(),
    }


def _wildfire_explanation(
    score: float,
    risk: str,
    features: Any,
    thresholds: dict[str, float],
    factors: dict[str, Any],
) -> dict[str, object]:
    current_temp = getattr(features, "temp_current_c", None)
    current_temp_text = ""
    if current_temp is not None:
        current_temp_text = f" Current temperature is {_c(current_temp)} C."
    return {
        "title": "Wildfire",
        "explanation": (
            f"Spontaneous wildfire risk index is {round(score * 100, 1)}% ({risk}). This is not a probability; it is a normalized warning index. "
            f"The model now requires dry fuel conditions plus ignition weather, then uses wind mainly as a spread amplifier. It combines historical dryness, "
            f"observed 24-hour maximum temperature {_c(features.temp_max_24h_c)} C, and wind gusts {_ms(features.wind_gust_max_ms)} m/s. "
            f"Risk rises sharply above {_c(thresholds['wildfire_temp_hot_c'])} C and above "
            f"{_ms(thresholds['wildfire_wind_high_ms'])} m/s. Historical dryness is compared with previous years for the same season, "
            f"so wind alone or one dry forecast day should not produce an excessive spontaneous-fire score.{current_temp_text}"
        ),
        "input_values": {
            "temperature_max_24h_c": _c(features.temp_max_24h_c),
            "temperature_current_c": None
            if current_temp is None
            else _c(current_temp),
            "temperature_forecast_max_next_24h_c": None
            if getattr(features, "temp_forecast_max_next_24h_c", None) is None
            else _c(features.temp_forecast_max_next_24h_c),
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
            "low": "temperatures below 25 C, weak wind, and moist soil",
            "medium": f"rising temperature, wind near {_ms(thresholds['wildfire_wind_high_ms'])} m/s, or drought signs",
            "high": f"temperature above {_c(thresholds['wildfire_temp_hot_c'])} C, wind above {_ms(thresholds['wildfire_wind_high_ms'])} m/s, and dry soil",
        },
        "risk_scale": _risk_scale(),
    }


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))
