from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class MonitorConfig:
    area_name: str
    polygon: list[list[float]]
    forecast_window_hours: int
    thresholds: dict[str, float]
    sensor_health: dict[str, float]
    copernicus: dict[str, object]
    context: dict[str, object]

    def with_area(self, area_name: str, polygon: list[list[float]]) -> "MonitorConfig":
        return MonitorConfig(
            area_name=area_name,
            polygon=polygon,
            forecast_window_hours=self.forecast_window_hours,
            thresholds=self.thresholds,
            sensor_health=self.sensor_health,
            copernicus=self.copernicus,
            context=self.context,
        )


def load_config(path: str | Path) -> MonitorConfig:
    raw: dict[str, Any] = json.loads(Path(path).read_text(encoding="utf-8"))
    weather_cfg = raw.get("weather", {})
    return MonitorConfig(
        area_name=raw["area"]["name"],
        polygon=raw["area"]["polygon"],
        forecast_window_hours=int(weather_cfg.get("forecast_window_hours", 24)),
        thresholds={k: float(v) for k, v in raw["thresholds"].items()},
        sensor_health={k: float(v) for k, v in raw["sensor_health"].items()},
        copernicus=raw.get("copernicus", {}),
        context=raw.get("context", {}),
    )
