from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from urllib.request import Request, urlopen

from .config import MonitorConfig


@dataclass(frozen=True)
class SatelliteProduct:
    id: str
    collection: str
    datetime: str | None
    platform: str | None
    cloud_cover: float | None


@dataclass(frozen=True)
class CollectionSummary:
    collection: str
    count: int
    latest_datetime: str | None
    products: list[SatelliteProduct]


@dataclass(frozen=True)
class CopernicusSummary:
    provider: str
    lookback_days: int
    sentinel1: CollectionSummary
    sentinel2: CollectionSummary
    auxiliary: list[CollectionSummary]
    flood_observation_ready: bool
    optical_observation_ready: bool
    evidence_explanation: str


def get_copernicus_summary(config: MonitorConfig, *, now: datetime) -> CopernicusSummary:
    lookback_days = int(config.copernicus.get("lookback_days", 14))
    limit = int(config.copernicus.get("limit_per_collection", 5))
    max_cloud = float(config.copernicus.get("sentinel2_max_cloud", 60))
    auxiliary_collections = [str(item) for item in config.copernicus.get("auxiliary_collections", [])]
    start = now.astimezone(timezone.utc) - timedelta(days=lookback_days)
    datetime_range = f"{_iso_z(start)}/{_iso_z(now)}"
    bbox = _bbox(config.polygon)

    sentinel1 = _search_collection(
        config,
        collection="sentinel-1-grd",
        bbox=bbox,
        datetime_range=datetime_range,
        limit=limit,
    )
    sentinel2 = _search_collection(
        config,
        collection="sentinel-2-l2a",
        bbox=bbox,
        datetime_range=datetime_range,
        limit=limit,
        cloud_cover_lte=max_cloud,
    )
    auxiliary = []
    for collection in auxiliary_collections:
        try:
            auxiliary.append(
                _search_collection(
                    config,
                    collection=collection,
                    bbox=bbox,
                    datetime_range=datetime_range,
                    limit=2,
                )
            )
        except Exception:
            auxiliary.append(
                CollectionSummary(
                    collection=collection,
                    count=0,
                    latest_datetime=None,
                    products=[],
                )
            )

    return CopernicusSummary(
        provider="Copernicus Data Space STAC",
        lookback_days=lookback_days,
        sentinel1=sentinel1,
        sentinel2=sentinel2,
        auxiliary=auxiliary,
        flood_observation_ready=sentinel1.count > 0,
        optical_observation_ready=sentinel2.count > 0,
        evidence_explanation=_evidence_explanation(sentinel1, sentinel2, auxiliary),
    )


def copernicus_to_dict(summary: CopernicusSummary) -> dict[str, object]:
    return asdict(summary)


def _search_collection(
    config: MonitorConfig,
    *,
    collection: str,
    bbox: list[float],
    datetime_range: str,
    limit: int,
    cloud_cover_lte: float | None = None,
) -> CollectionSummary:
    body: dict[str, object] = {
        "collections": [collection],
        "bbox": bbox,
        "datetime": datetime_range,
        "limit": limit,
        "sortby": [{"field": "properties.datetime", "direction": "desc"}],
        "fields": {
            "include": [
                "id",
                "collection",
                "properties.datetime",
                "properties.platform",
                "properties.eo:cloud_cover",
            ]
        },
    }
    if cloud_cover_lte is not None:
        body["query"] = {"eo:cloud_cover": {"lte": cloud_cover_lte}}

    payload = _post_json(str(config.copernicus.get("stac_url")), body)
    products = [_product_from_feature(feature) for feature in payload.get("features", [])]
    latest = products[0].datetime if products else None
    return CollectionSummary(
        collection=collection,
        count=len(products),
        latest_datetime=latest,
        products=products,
    )


def _evidence_explanation(
    sentinel1: CollectionSummary,
    sentinel2: CollectionSummary,
    auxiliary: list[CollectionSummary],
) -> str:
    available_aux = [item.collection for item in auxiliary if item.count > 0]
    parts = [
        f"Sentinel-1 radar products found: {sentinel1.count}",
        f"Sentinel-2 optical products found: {sentinel2.count}",
    ]
    if available_aux:
        parts.append("additional evidence collections found: " + ", ".join(available_aux[:8]))
    else:
        parts.append("no auxiliary Copernicus/CLMS/MODIS evidence products found in the lookback window")
    return "; ".join(parts)


def _post_json(url: str, body: dict[str, object]) -> dict[str, object]:
    request = Request(
        url,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=45) as response:
        return json.loads(response.read().decode("utf-8"))


def _product_from_feature(feature: dict[str, object]) -> SatelliteProduct:
    properties = feature.get("properties") or {}
    if not isinstance(properties, dict):
        properties = {}
    cloud_cover = properties.get("eo:cloud_cover")
    return SatelliteProduct(
        id=str(feature.get("id", "")),
        collection=str(feature.get("collection", "")),
        datetime=_optional_str(properties.get("datetime")),
        platform=_optional_str(properties.get("platform")),
        cloud_cover=None if cloud_cover is None else float(cloud_cover),
    )


def _optional_str(value: object) -> str | None:
    return None if value is None else str(value)


def _bbox(polygon: list[list[float]]) -> list[float]:
    longitudes = [point[0] for point in polygon]
    latitudes = [point[1] for point in polygon]
    return [min(longitudes), min(latitudes), max(longitudes), max(latitudes)]


def _iso_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
