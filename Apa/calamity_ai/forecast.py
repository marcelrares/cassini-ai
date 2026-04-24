from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from statistics import mean
from urllib.parse import urlencode
from urllib.request import urlopen

from .config import MonitorConfig
from .scoring import WeatherFeatures, score_calamities


@dataclass(frozen=True)
class DailyPrediction:
    date: str
    weather: WeatherFeatures
    calamities: dict[str, dict[str, object]]
    summary: str


@dataclass(frozen=True)
class MultiDayPrediction:
    days: int
    model_note: str
    daily: list[DailyPrediction]


def get_open_meteo_predictions(
    config: MonitorConfig,
    *,
    context: object | None,
    days: int = 5,
) -> MultiDayPrediction:
    longitude = mean(point[0] for point in config.polygon)
    latitude = mean(point[1] for point in config.polygon)
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "forecast_days": days,
        "timezone": "UTC",
        "daily": ",".join(
            [
                "temperature_2m_max",
                "temperature_2m_min",
                "precipitation_sum",
                "wind_gusts_10m_max",
                "et0_fao_evapotranspiration",
            ]
        ),
    }
    with urlopen("https://api.open-meteo.com/v1/forecast?" + urlencode(params), timeout=45) as response:
        payload = json.loads(response.read().decode("utf-8"))
    daily = payload["daily"]
    predictions = []
    for index, date in enumerate(daily["time"][:days]):
        temp_max = _value(daily, "temperature_2m_max", index, 0.0)
        temp_min = _value(daily, "temperature_2m_min", index, temp_max)
        precip_mm = _value(daily, "precipitation_sum", index, 0.0)
        wind_gust_kmh = _value(daily, "wind_gusts_10m_max", index, 0.0)
        evapotranspiration = _value(daily, "et0_fao_evapotranspiration", index, 0.0)
        features = WeatherFeatures(
            precip_24h_m=precip_mm / 1000,
            temp_mean_24h_c=(temp_max + temp_min) / 2,
            temp_max_24h_c=temp_max,
            wind_gust_max_ms=wind_gust_kmh / 3.6,
            cape_max_jkg=0,
            soil_moisture_proxy=None,
            relative_humidity_mean_percent=None,
            evapotranspiration_24h_mm=evapotranspiration,
            vapor_pressure_deficit_kpa=None,
        )
        calamities = score_calamities(features, config.thresholds, context=context)
        predictions.append(
            DailyPrediction(
                date=str(date),
                weather=features,
                calamities=calamities,
                summary=_summary(date, calamities),
            )
        )
    return MultiDayPrediction(
        days=len(predictions),
        model_note=(
            "Forward predictions are warning-index forecasts, not probabilities. They use Open-Meteo daily forecast values "
            "combined with the same historical baseline and terrain context used by the current report."
        ),
        daily=predictions,
    )


def predictions_to_dict(predictions: MultiDayPrediction) -> dict[str, object]:
    return asdict(predictions)


def _value(daily: dict[str, list[object]], key: str, index: int, default: float) -> float:
    values = daily.get(key, [])
    if index >= len(values) or values[index] is None:
        return default
    return float(values[index])


def _summary(date: object, calamities: dict[str, dict[str, object]]) -> str:
    ranked = sorted(
        ((name, float(data["risk_index_percent"]), str(data["risk"])) for name, data in calamities.items()),
        key=lambda item: item[1],
        reverse=True,
    )
    top = ", ".join(f"{name} {round(value, 1)} ({risk})" for name, value, risk in ranked[:3])
    return f"{date}: highest forecast indices are {top}."
