from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from statistics import mean
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen

from .config import MonitorConfig
from .geo import centroid
from .scoring import WeatherFeatures


def demo_weather_features() -> WeatherFeatures:
    return WeatherFeatures(
        precip_24h_m=0.026,
        temp_mean_24h_c=28.4,
        temp_max_24h_c=34.2,
        wind_gust_max_ms=14.5,
        cape_max_jkg=1100,
        temp_current_c=27.6,
        temp_forecast_max_next_24h_c=34.2,
        soil_moisture_proxy=0.31,
        relative_humidity_mean_percent=36.0,
        evapotranspiration_24h_mm=4.8,
        vapor_pressure_deficit_kpa=1.9,
        # Satellite fields initialized to None (will be enriched if Copernicus available)
        satellite_water_index=None,
        satellite_soil_moisture_anomaly=None,
        satellite_fire_radiative_power=None,
        satellite_burned_area_fraction=None,
        satellite_land_surface_temp_anomaly=None,
        satellite_ndvi_anomaly=None,
        satellite_optical_quality=None,
        satellite_radar_confidence=None,
    )

def get_open_meteo_weather_features(config: MonitorConfig) -> WeatherFeatures:
    longitude, latitude = centroid(config.polygon)
    hourly_variables = [
        "temperature_2m",
        "precipitation",
        "wind_gusts_10m",
        "relative_humidity_2m",
        "evapotranspiration",
        "vapor_pressure_deficit",
        "soil_moisture_0_to_1cm",
    ]
    payload = _fetch_open_meteo(latitude, longitude, hourly_variables, forecast_hours=config.forecast_window_hours)
    hourly = payload["hourly"]
    past_indices, forecast_indices = _split_hourly_indices(hourly.get("time", []), payload.get("current", {}))
    past_indices = past_indices[-config.forecast_window_hours :]
    forecast_indices = forecast_indices[: config.forecast_window_hours]
    if not past_indices:
        past_indices = list(range(max(1, min(config.forecast_window_hours, len(hourly["time"])))))
    if not forecast_indices:
        forecast_indices = past_indices

    observed_temperatures = _numbers_at(hourly["temperature_2m"], past_indices)
    forecast_temperatures = _numbers_at(hourly["temperature_2m"], forecast_indices)
    precipitation_mm = _numbers_at(hourly["precipitation"], forecast_indices)
    wind_gusts = _numbers_at(hourly["wind_gusts_10m"], forecast_indices)
    humidity = _optional_numbers_at(hourly.get("relative_humidity_2m", []), forecast_indices)
    evapotranspiration = _optional_numbers_at(hourly.get("evapotranspiration", []), forecast_indices)
    vapor_pressure_deficit = _optional_numbers_at(hourly.get("vapor_pressure_deficit", []), forecast_indices)
    soil_values = _optional_numbers_at(hourly.get("soil_moisture_0_to_1cm", []), forecast_indices)
    current = payload.get("current", {})
    current_temp = None
    if isinstance(current, dict) and current.get("temperature_2m") is not None:
        current_temp = float(current["temperature_2m"])
        observed_temperatures.append(current_temp)

    soil_proxy = None
    if soil_values:
        soil_proxy = max(0.0, min(1.0, mean(soil_values) / 0.5))

    return WeatherFeatures(
        precip_24h_m=sum(precipitation_mm) / 1000,
        temp_mean_24h_c=mean(observed_temperatures),
        temp_max_24h_c=max(observed_temperatures),
        wind_gust_max_ms=max(wind_gusts) / 3.6,
        cape_max_jkg=0,
        temp_current_c=current_temp,
        temp_forecast_max_next_24h_c=max(forecast_temperatures) if forecast_temperatures else None,
        soil_moisture_proxy=soil_proxy,
        relative_humidity_mean_percent=None if not humidity else mean(humidity),
        evapotranspiration_24h_mm=None if not evapotranspiration else sum(evapotranspiration),
        vapor_pressure_deficit_kpa=None if not vapor_pressure_deficit else mean(vapor_pressure_deficit),
        # Satellite fields remain None (enriched later if available)
        satellite_water_index=None,
        satellite_soil_moisture_anomaly=None,
        satellite_fire_radiative_power=None,
        satellite_burned_area_fraction=None,
        satellite_land_surface_temp_anomaly=None,
        satellite_ndvi_anomaly=None,
        satellite_optical_quality=None,
        satellite_radar_confidence=None,
    )


def _fetch_open_meteo(
    latitude: float,
    longitude: float,
    hourly_variables: list[str],
    *,
    forecast_hours: int,
) -> dict[str, object]:
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": "temperature_2m",
        "hourly": ",".join(hourly_variables),
        "past_hours": forecast_hours,
        "forecast_hours": forecast_hours,
        "timezone": "auto",
    }
    url = "https://api.open-meteo.com/v1/forecast?" + urlencode(params)
    try:
        with urlopen(url, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        if exc.code == 400:
            for optional in [
                "soil_moisture_0_to_1cm",
                "vapor_pressure_deficit",
                "evapotranspiration",
                "relative_humidity_2m",
            ]:
                if optional in hourly_variables:
                    return _fetch_open_meteo(
                        latitude,
                        longitude,
                        [item for item in hourly_variables if item != optional],
                        forecast_hours=forecast_hours,
                    )
        raise


def _first_numbers(values: list[float | int | None], count: int) -> list[float]:
    numbers = [float(value) for value in values[:count] if value is not None]
    if not numbers:
        raise RuntimeError("Weather provider returned no usable hourly values.")
    return numbers


def _optional_first_numbers(values: list[float | int | None], count: int) -> list[float]:
    return [float(value) for value in values[:count] if value is not None]


def _numbers_at(values: list[float | int | None], indices: list[int]) -> list[float]:
    numbers = _optional_numbers_at(values, indices)
    if not numbers:
        raise RuntimeError("Weather provider returned no usable hourly values.")
    return numbers


def _optional_numbers_at(values: list[float | int | None], indices: list[int]) -> list[float]:
    return [float(values[index]) for index in indices if index < len(values) and values[index] is not None]


def _split_hourly_indices(times: list[object], current: object) -> tuple[list[int], list[int]]:
    current_time = None
    if isinstance(current, dict) and isinstance(current.get("time"), str):
        current_time = _parse_open_meteo_hour(current["time"])
    if current_time is None:
        midpoint = len(times) // 2
        return list(range(midpoint)), list(range(midpoint, len(times)))

    past_indices = []
    forecast_indices = []
    for index, raw_time in enumerate(times):
        if not isinstance(raw_time, str):
            continue
        hour_time = _parse_open_meteo_hour(raw_time)
        if hour_time is None:
            continue
        if hour_time <= current_time:
            past_indices.append(index)
        else:
            forecast_indices.append(index)
    return past_indices, forecast_indices


def _parse_open_meteo_hour(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value).replace(minute=0, second=0, microsecond=0)
    except ValueError:
        return None


def features_to_dict(features: WeatherFeatures) -> dict[str, float | None]:
    return asdict(features)
