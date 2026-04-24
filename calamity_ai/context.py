from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from statistics import mean
from urllib.parse import urlencode
from urllib.request import urlopen

from .config import MonitorConfig
from .geo import centroid, numbers


@dataclass(frozen=True)
class SeasonalBaselineContext:
    baseline_years: list[int]
    precipitation_7d_normal_mm: float | None
    precipitation_30d_normal_mm: float | None
    precipitation_90d_normal_mm: float | None
    dry_days_30d_normal: float | None
    dry_days_90d_normal: float | None
    evapotranspiration_30d_normal_mm: float | None
    evapotranspiration_90d_normal_mm: float | None
    precipitation_7d_ratio: float | None
    precipitation_30d_ratio: float | None
    precipitation_90d_ratio: float | None
    dry_days_ratio: float | None
    dry_days_90d_ratio: float | None
    evapotranspiration_ratio: float | None
    evapotranspiration_90d_ratio: float | None
    explanation: str


@dataclass(frozen=True)
class HistoricalWeatherContext:
    lookback_days: int
    precipitation_7d_mm: float | None
    precipitation_30d_mm: float | None
    precipitation_90d_mm: float | None
    dry_days_30d: int | None
    dry_days_90d: int | None
    hot_days_30d: int | None
    hot_days_90d: int | None
    max_temperature_30d_c: float | None
    max_temperature_90d_c: float | None
    max_wind_gust_30d_ms: float | None
    max_wind_gust_90d_ms: float | None
    evapotranspiration_30d_mm: float | None
    evapotranspiration_90d_mm: float | None
    seasonal_baseline: SeasonalBaselineContext
    explanation: str


@dataclass(frozen=True)
class ElevationContext:
    center_elevation_m: float | None
    min_elevation_m: float | None
    max_elevation_m: float | None
    elevation_range_m: float | None
    terrain_class: str
    explanation: str


@dataclass(frozen=True)
class EnvironmentalContext:
    history: HistoricalWeatherContext
    elevation: ElevationContext


def get_environmental_context(config: MonitorConfig, *, now: datetime) -> EnvironmentalContext:
    return EnvironmentalContext(
        history=get_historical_weather_context(config, now=now),
        elevation=get_elevation_context(config),
    )


def environmental_context_to_dict(context: EnvironmentalContext) -> dict[str, object]:
    return asdict(context)


def get_historical_weather_context(config: MonitorConfig, *, now: datetime) -> HistoricalWeatherContext:
    longitude, latitude = centroid(config.polygon)
    lookback_days = int(config.context.get("recent_lookback_days", 90))
    baseline_years_count = int(config.context.get("baseline_years", 10))
    end = now.astimezone(timezone.utc).date() - timedelta(days=1)
    start = end - timedelta(days=lookback_days - 1)
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "daily": ",".join(
            [
                "precipitation_sum",
                "temperature_2m_max",
                "wind_gusts_10m_max",
                "et0_fao_evapotranspiration",
            ]
        ),
        "timezone": "UTC",
    }
    try:
        payload = _get_json("https://archive-api.open-meteo.com/v1/archive", params)
        daily = payload["daily"]
        precipitation = numbers(daily.get("precipitation_sum", []))
        temperature_max = numbers(daily.get("temperature_2m_max", []))
        wind_gusts_kmh = numbers(daily.get("wind_gusts_10m_max", []))
        evapotranspiration = numbers(daily.get("et0_fao_evapotranspiration", []))
    except Exception as exc:
        empty_baseline = SeasonalBaselineContext(
            baseline_years=[],
            precipitation_7d_normal_mm=None,
            precipitation_30d_normal_mm=None,
            precipitation_90d_normal_mm=None,
            dry_days_30d_normal=None,
            dry_days_90d_normal=None,
            evapotranspiration_30d_normal_mm=None,
            evapotranspiration_90d_normal_mm=None,
            precipitation_7d_ratio=None,
            precipitation_30d_ratio=None,
            precipitation_90d_ratio=None,
            dry_days_ratio=None,
            dry_days_90d_ratio=None,
            evapotranspiration_ratio=None,
            evapotranspiration_90d_ratio=None,
            explanation="Seasonal baseline could not be computed because recent history was unavailable.",
        )
        return HistoricalWeatherContext(
            lookback_days=lookback_days,
            precipitation_7d_mm=None,
            precipitation_30d_mm=None,
            precipitation_90d_mm=None,
            dry_days_30d=None,
            dry_days_90d=None,
            hot_days_30d=None,
            hot_days_90d=None,
            max_temperature_30d_c=None,
            max_temperature_90d_c=None,
            max_wind_gust_30d_ms=None,
            max_wind_gust_90d_ms=None,
            evapotranspiration_30d_mm=None,
            evapotranspiration_90d_mm=None,
            seasonal_baseline=empty_baseline,
            explanation=f"Historical context could not be loaded: {exc}",
        )

    precipitation_7d = sum(precipitation[-7:]) if precipitation else None
    precipitation_30d = sum(precipitation[-30:]) if precipitation else None
    precipitation_90d = sum(precipitation) if precipitation else None
    dry_days_30d = sum(1 for value in precipitation[-30:] if value < 1.0) if precipitation else None
    dry_days_90d = sum(1 for value in precipitation if value < 1.0) if precipitation else None
    hot_days_30d = sum(1 for value in temperature_max[-30:] if value >= 30.0) if temperature_max else None
    hot_days_90d = sum(1 for value in temperature_max if value >= 30.0) if temperature_max else None
    max_temp_30d = max(temperature_max[-30:]) if temperature_max else None
    max_temp_90d = max(temperature_max) if temperature_max else None
    max_wind_30d_ms = max(wind_gusts_kmh[-30:]) / 3.6 if wind_gusts_kmh else None
    max_wind_90d_ms = max(wind_gusts_kmh) / 3.6 if wind_gusts_kmh else None
    et0_30d = sum(evapotranspiration[-30:]) if evapotranspiration else None
    et0_90d = sum(evapotranspiration) if evapotranspiration else None

    explanation_parts = []
    if precipitation_30d is not None:
        explanation_parts.append(f"last 30 days rainfall: {round(precipitation_30d, 1)} mm")
    if precipitation_90d is not None:
        explanation_parts.append(f"last {lookback_days} days rainfall: {round(precipitation_90d, 1)} mm")
    if dry_days_30d is not None:
        explanation_parts.append(f"dry days under 1 mm rain: {dry_days_30d}/30")
    if et0_30d is not None:
        explanation_parts.append(f"reference evapotranspiration: {round(et0_30d, 1)} mm/30d")
    if max_wind_90d_ms is not None:
        explanation_parts.append(f"max historical gust: {round(max_wind_90d_ms, 1)} m/s")
    baseline = get_seasonal_baseline_context(
        latitude=latitude,
        longitude=longitude,
        current_start=start,
        current_end=end,
        baseline_years_count=baseline_years_count,
        current_precipitation_7d_mm=precipitation_7d,
        current_precipitation_30d_mm=precipitation_30d,
        current_precipitation_90d_mm=precipitation_90d,
        current_dry_days_30d=dry_days_30d,
        current_dry_days_90d=dry_days_90d,
        current_evapotranspiration_30d_mm=et0_30d,
        current_evapotranspiration_90d_mm=et0_90d,
    )

    return HistoricalWeatherContext(
        lookback_days=lookback_days,
        precipitation_7d_mm=_round_or_none(precipitation_7d),
        precipitation_30d_mm=_round_or_none(precipitation_30d),
        precipitation_90d_mm=_round_or_none(precipitation_90d),
        dry_days_30d=dry_days_30d,
        dry_days_90d=dry_days_90d,
        hot_days_30d=hot_days_30d,
        hot_days_90d=hot_days_90d,
        max_temperature_30d_c=_round_or_none(max_temp_30d),
        max_temperature_90d_c=_round_or_none(max_temp_90d),
        max_wind_gust_30d_ms=_round_or_none(max_wind_30d_ms),
        max_wind_gust_90d_ms=_round_or_none(max_wind_90d_ms),
        evapotranspiration_30d_mm=_round_or_none(et0_30d),
        evapotranspiration_90d_mm=_round_or_none(et0_90d),
        seasonal_baseline=baseline,
        explanation="Historical context: " + "; ".join(explanation_parts) if explanation_parts else "No historical values available.",
    )


def get_seasonal_baseline_context(
    *,
    latitude: float,
    longitude: float,
    current_start: date,
    current_end: date,
    baseline_years_count: int,
    current_precipitation_7d_mm: float | None,
    current_precipitation_30d_mm: float | None,
    current_precipitation_90d_mm: float | None,
    current_dry_days_30d: int | None,
    current_dry_days_90d: int | None,
    current_evapotranspiration_30d_mm: float | None,
    current_evapotranspiration_90d_mm: float | None,
) -> SeasonalBaselineContext:
    baseline_years = [current_end.year - offset for offset in range(1, baseline_years_count + 1)]
    samples: list[dict[str, float]] = []
    for year in baseline_years:
        try:
            start = _same_month_day(current_start, year)
            end = _same_month_day(current_end, year)
            daily = _load_daily_archive(latitude, longitude, start, end)
            precipitation = numbers(daily.get("precipitation_sum", []))
            evapotranspiration = numbers(daily.get("et0_fao_evapotranspiration", []))
            if not precipitation:
                continue
            samples.append(
                {
                    "precipitation_7d_mm": sum(precipitation[-7:]),
                    "precipitation_30d_mm": sum(precipitation[-30:]),
                    "precipitation_90d_mm": sum(precipitation),
                    "dry_days_30d": float(sum(1 for value in precipitation[-30:] if value < 1.0)),
                    "dry_days_90d": float(sum(1 for value in precipitation if value < 1.0)),
                    "evapotranspiration_30d_mm": sum(evapotranspiration[-30:]) if evapotranspiration else 0.0,
                    "evapotranspiration_90d_mm": sum(evapotranspiration) if evapotranspiration else 0.0,
                }
            )
        except Exception:
            continue

    if not samples:
        return SeasonalBaselineContext(
            baseline_years=[],
            precipitation_7d_normal_mm=None,
            precipitation_30d_normal_mm=None,
            precipitation_90d_normal_mm=None,
            dry_days_30d_normal=None,
            dry_days_90d_normal=None,
            evapotranspiration_30d_normal_mm=None,
            evapotranspiration_90d_normal_mm=None,
            precipitation_7d_ratio=None,
            precipitation_30d_ratio=None,
            precipitation_90d_ratio=None,
            dry_days_ratio=None,
            dry_days_90d_ratio=None,
            evapotranspiration_ratio=None,
            evapotranspiration_90d_ratio=None,
            explanation="Seasonal baseline could not be computed from previous years.",
        )

    precip_7d_normal = mean(sample["precipitation_7d_mm"] for sample in samples)
    precip_30d_normal = mean(sample["precipitation_30d_mm"] for sample in samples)
    precip_90d_normal = mean(sample["precipitation_90d_mm"] for sample in samples)
    dry_days_normal = mean(sample["dry_days_30d"] for sample in samples)
    dry_days_90d_normal = mean(sample["dry_days_90d"] for sample in samples)
    et0_normal = mean(sample["evapotranspiration_30d_mm"] for sample in samples)
    et0_90d_normal = mean(sample["evapotranspiration_90d_mm"] for sample in samples)
    precip_7d_ratio = _safe_ratio(current_precipitation_7d_mm, precip_7d_normal)
    precip_30d_ratio = _safe_ratio(current_precipitation_30d_mm, precip_30d_normal)
    precip_90d_ratio = _safe_ratio(current_precipitation_90d_mm, precip_90d_normal)
    dry_days_ratio = _safe_ratio(float(current_dry_days_30d) if current_dry_days_30d is not None else None, dry_days_normal)
    dry_days_90d_ratio = _safe_ratio(float(current_dry_days_90d) if current_dry_days_90d is not None else None, dry_days_90d_normal)
    et0_ratio = _safe_ratio(current_evapotranspiration_30d_mm, et0_normal)
    et0_90d_ratio = _safe_ratio(current_evapotranspiration_90d_mm, et0_90d_normal)
    used_years = baseline_years[: len(samples)]
    return SeasonalBaselineContext(
        baseline_years=used_years,
        precipitation_7d_normal_mm=_round_or_none(precip_7d_normal),
        precipitation_30d_normal_mm=_round_or_none(precip_30d_normal),
        precipitation_90d_normal_mm=_round_or_none(precip_90d_normal),
        dry_days_30d_normal=_round_or_none(dry_days_normal),
        dry_days_90d_normal=_round_or_none(dry_days_90d_normal),
        evapotranspiration_30d_normal_mm=_round_or_none(et0_normal),
        evapotranspiration_90d_normal_mm=_round_or_none(et0_90d_normal),
        precipitation_7d_ratio=_round_or_none(precip_7d_ratio),
        precipitation_30d_ratio=_round_or_none(precip_30d_ratio),
        precipitation_90d_ratio=_round_or_none(precip_90d_ratio),
        dry_days_ratio=_round_or_none(dry_days_ratio),
        dry_days_90d_ratio=_round_or_none(dry_days_90d_ratio),
        evapotranspiration_ratio=_round_or_none(et0_ratio),
        evapotranspiration_90d_ratio=_round_or_none(et0_90d_ratio),
        explanation=(
            f"Seasonal baseline from previous {len(samples)} years: 30-day rainfall normal {round(precip_30d_normal, 1)} mm, "
            f"current ratio {round(precip_30d_ratio, 2) if precip_30d_ratio is not None else 'n/a'}; "
            f"{(current_end - current_start).days + 1}-day rainfall normal {round(precip_90d_normal, 1)} mm, "
            f"current ratio {round(precip_90d_ratio, 2) if precip_90d_ratio is not None else 'n/a'}; "
            f"dry-days normal {round(dry_days_normal, 1)}, current ratio {round(dry_days_ratio, 2) if dry_days_ratio is not None else 'n/a'}."
        ),
    )


def _load_daily_archive(latitude: float, longitude: float, start: date, end: date) -> dict[str, object]:
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "daily": ",".join(
            [
                "precipitation_sum",
                "temperature_2m_max",
                "wind_gusts_10m_max",
                "et0_fao_evapotranspiration",
            ]
        ),
        "timezone": "UTC",
    }
    payload = _get_json("https://archive-api.open-meteo.com/v1/archive", params)
    return payload["daily"]


def get_elevation_context(config: MonitorConfig) -> ElevationContext:
    center_lon, center_lat = centroid(config.polygon)
    sample_points = [[center_lon, center_lat], *config.polygon[:4]]
    params = {
        "latitude": ",".join(str(point[1]) for point in sample_points),
        "longitude": ",".join(str(point[0]) for point in sample_points),
    }
    try:
        payload = _get_json("https://api.open-meteo.com/v1/elevation", params)
        elevations = numbers(payload.get("elevation", []))
    except Exception as exc:
        return ElevationContext(
            center_elevation_m=None,
            min_elevation_m=None,
            max_elevation_m=None,
            elevation_range_m=None,
            terrain_class="unknown",
            explanation=f"Elevation context could not be loaded: {exc}",
        )

    center_elevation = elevations[0] if elevations else None
    min_elevation = min(elevations) if elevations else None
    max_elevation = max(elevations) if elevations else None
    elevation_range = max_elevation - min_elevation if min_elevation is not None and max_elevation is not None else None
    terrain_class = _terrain_class(elevation_range)
    explanation = (
        f"Elevation context: center is about {round(center_elevation, 1)} m, sampled range is "
        f"{round(min_elevation, 1)}-{round(max_elevation, 1)} m, terrain class is {terrain_class}. "
        "Steeper terrain can increase runoff speed and local flood response; flatter terrain can hold standing water longer."
        if center_elevation is not None and min_elevation is not None and max_elevation is not None
        else "No elevation values available."
    )
    return ElevationContext(
        center_elevation_m=_round_or_none(center_elevation),
        min_elevation_m=_round_or_none(min_elevation),
        max_elevation_m=_round_or_none(max_elevation),
        elevation_range_m=_round_or_none(elevation_range),
        terrain_class=terrain_class,
        explanation=explanation,
    )


def _get_json(url: str, params: dict[str, object]) -> dict[str, object]:
    with urlopen(url + "?" + urlencode(params), timeout=45) as response:
        return json.loads(response.read().decode("utf-8"))


def _round_or_none(value: float | None) -> float | None:
    return None if value is None else round(value, 1)


def _safe_ratio(value: float | None, normal: float | None) -> float | None:
    if value is None or normal is None or normal <= 0:
        return None
    return value / normal


def _same_month_day(value: date, year: int) -> date:
    try:
        return value.replace(year=year)
    except ValueError:
        return value.replace(year=year, day=28)


def _terrain_class(elevation_range_m: float | None) -> str:
    if elevation_range_m is None:
        return "unknown"
    if elevation_range_m < 30:
        return "flat"
    if elevation_range_m < 120:
        return "rolling"
    return "steep"
