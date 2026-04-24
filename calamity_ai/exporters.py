from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


def append_jsonl(path: str | Path, report: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(report, sort_keys=True) + "\n")


def append_csv(path: str | Path, report: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    row = flatten_report(report)
    write_header = not target.exists()
    with target.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def flatten_report(report: dict[str, Any]) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for key, value in report.items():
        if isinstance(value, dict):
            for subkey, subvalue in value.items():
                if isinstance(subvalue, dict):
                    for leaf_key, leaf_value in subvalue.items():
                        row[f"{key}_{subkey}_{leaf_key}"] = leaf_value
                else:
                    row[f"{key}_{subkey}"] = subvalue
        else:
            row[key] = value
    return row
