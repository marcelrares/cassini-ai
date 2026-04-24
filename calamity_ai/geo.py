from __future__ import annotations

from statistics import mean


Point = list[float]


def centroid(polygon: list[Point]) -> tuple[float, float]:
    return mean(point[0] for point in polygon), mean(point[1] for point in polygon)


def bbox(polygon: list[Point]) -> tuple[float, float, float, float]:
    longitudes = [point[0] for point in polygon]
    latitudes = [point[1] for point in polygon]
    return min(longitudes), min(latitudes), max(longitudes), max(latitudes)


def numbers(values: list[float | int | None]) -> list[float]:
    return [float(value) for value in values if value is not None]
