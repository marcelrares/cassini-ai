from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class MonitorConfig:
    project_id: str
    area_name: str
    polygon: list[list[float]]
    dataset: str
    scale_m: int
    forecast_window_hours: int
    thresholds: dict[str, float]
    sensor_health: dict[str, float]
    copernicus: dict[str, object]
    context: dict[str, object]
    zones: dict[str, object]

    def with_project_id(self, project_id: str | None) -> "MonitorConfig":
        return MonitorConfig(
            project_id=project_id or self.project_id,
            area_name=self.area_name,
            polygon=self.polygon,
            dataset=self.dataset,
            scale_m=self.scale_m,
            forecast_window_hours=self.forecast_window_hours,
            thresholds=self.thresholds,
            sensor_health=self.sensor_health,
            copernicus=self.copernicus,
            context=self.context,
            zones=self.zones,
        )

    def with_area(self, area_name: str, polygon: list[list[float]]) -> "MonitorConfig":
        return MonitorConfig(
            project_id=self.project_id,
            area_name=area_name,
            polygon=polygon,
            dataset=self.dataset,
            scale_m=self.scale_m,
            forecast_window_hours=self.forecast_window_hours,
            thresholds=self.thresholds,
            sensor_health=self.sensor_health,
            copernicus=self.copernicus,
            context=self.context,
            zones=self.zones,
        )


def load_config(path: str | Path) -> MonitorConfig:
    raw: dict[str, Any] = json.loads(Path(path).read_text(encoding="utf-8"))
    ee_cfg = raw["earth_engine"]
    return MonitorConfig(
        project_id=raw["project_id"],
        area_name=raw["area"]["name"],
        polygon=raw["area"]["polygon"],
        dataset=ee_cfg["dataset"],
        scale_m=int(ee_cfg["scale_m"]),
        forecast_window_hours=int(ee_cfg["forecast_window_hours"]),
        thresholds={k: float(v) for k, v in raw["thresholds"].items()},
        sensor_health={k: float(v) for k, v in raw["sensor_health"].items()},
        copernicus=raw.get("copernicus", {}),
        context=raw.get("context", {}),
        zones=raw.get("zones", {}),
    )
