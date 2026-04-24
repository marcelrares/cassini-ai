from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class SensorSummary:
    total: int
    online: int
    offline: int
    stale: int
    working: bool


def _parse_utc(value: str) -> datetime:
    if value.strip().upper() == "NOW":
        return datetime.now(timezone.utc)
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def summarize_sensors(
    csv_path: str | Path,
    *,
    now: datetime,
    min_online_ratio: float,
    stale_after_minutes: float,
) -> SensorSummary:
    rows = list(csv.DictReader(Path(csv_path).read_text(encoding="utf-8").splitlines()))
    total = len(rows)
    stale = 0
    online = 0

    for row in rows:
        is_online = row["status"].strip().lower() == "online"
        last_seen = _parse_utc(row["last_seen_utc"])
        age_minutes = (now.astimezone(timezone.utc) - last_seen).total_seconds() / 60
        is_stale = age_minutes > stale_after_minutes
        stale += int(is_stale)
        online += int(is_online and not is_stale)

    offline = total - online
    online_ratio = online / total if total else 0
    return SensorSummary(
        total=total,
        online=online,
        offline=offline,
        stale=stale,
        working=online_ratio >= min_online_ratio,
    )
