from __future__ import annotations

import csv
import json
import os
import re
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .config import MonitorConfig
from .geo import bbox


LAND_COVER_CACHE_DIR = Path("data/land_cover")
SUPPORTED_LAND_COVER_SUFFIXES = {".tif", ".tiff", ".geojson", ".json", ".csv"}
CLMS_LCM10_COLLECTION_ID = "828f6b20-8ffd-48f8-a1da-fefd271456db"
CLMS_PROCESS_API_URL = "https://sh.dataspace.copernicus.eu/api/v1/process"
CDSE_TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
DEFAULT_MAX_LAND_COVER_PIXELS = 4_000_000

LAND_COVER_CLASSES = {
    "forest": "forest",
    "tree_cover": "forest",
    "trees": "forest",
    "urban": "urban",
    "built": "urban",
    "built_up": "urban",
    "settlement": "urban",
    "grass": "field_plain",
    "grassland": "field_plain",
    "meadow": "field_plain",
    "plain": "field_plain",
    "cropland": "agriculture",
    "crop": "agriculture",
    "agriculture": "agriculture",
    "farmland": "agriculture",
    "water": "water",
    "wetland": "wetland",
}

CLMS_LCM10_CODES = {
    10: "forest",
    20: "field_plain",
    30: "field_plain",
    40: "agriculture",
    50: "wetland",
    60: "wetland",
    70: "field_plain",
    80: "field_plain",
    90: "urban",
    100: "water",
}


@dataclass(frozen=True)
class SatelliteLandCoverSummary:
    available: bool
    basis: str
    source: dict[str, object]
    percentages: dict[str, float | None]
    hectares: dict[str, float | None]
    crop_percentages: dict[str, float]
    crop_hectares: dict[str, float]
    crop_detail_available: bool
    fallback: dict[str, object]
    notes: str


def prepare_land_cover_source(
    config: MonitorConfig,
    explicit_path: str | None,
    *,
    cache_dir: str | Path = LAND_COVER_CACHE_DIR,
    keep_latest: int = 2,
    auto_download: bool = True,
    max_pixels: int = DEFAULT_MAX_LAND_COVER_PIXELS,
    year: int | None = None,
) -> str | None:
    area_dir = _area_cache_dir(config, Path(cache_dir))
    area_dir.mkdir(parents=True, exist_ok=True)
    _cleanup_old_land_cover(area_dir, keep_latest=keep_latest)
    if explicit_path:
        return explicit_path
    if _copernicus_lcfm_files(area_dir):
        return str(area_dir)
    latest = _latest_land_cover_file(area_dir)
    if latest:
        return str(latest if latest.is_file() else area_dir)
    if auto_download:
        downloaded = download_copernicus_lcfm_land_cover(config, area_dir, max_pixels=max_pixels, year=year)
        if downloaded:
            return str(downloaded)
    return None


def download_copernicus_lcfm_land_cover(
    config: MonitorConfig,
    output_dir: str | Path,
    *,
    max_pixels: int = DEFAULT_MAX_LAND_COVER_PIXELS,
    year: int | None = None,
) -> Path | None:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    product_year = year or __import__("datetime").datetime.now(__import__("datetime").timezone.utc).year
    width, height = _process_raster_dimensions(config, max_pixels=max_pixels)
    if width * height > max_pixels:
        _write_download_manifest(
            output_path,
            {
                "status": "skipped",
                "provider": "Copernicus CLMS LCFM LCM-10",
                "reason": "bbox needs too many output pixels for automatic Copernicus Process API download",
                "pixel_count": width * height,
                "max_pixels": max_pixels,
            },
        )
        return None
    token = _cdse_access_token()
    if not token:
        _write_download_manifest(
            output_path,
            {
                "status": "credentials_missing",
                "provider": "Copernicus CLMS LCFM LCM-10",
                "collection_id": CLMS_LCM10_COLLECTION_ID,
                "reason": "Set CDSE_SH_CLIENT_ID/CDSE_SH_CLIENT_SECRET or SENTINELHUB_CLIENT_ID/SENTINELHUB_CLIENT_SECRET to download CLMS land-cover rasters.",
            },
        )
        return None
    target = output_path / f"LCFM_LCM-10_V1_{product_year}_{_bbox_slug(config)}_MAP.tif"
    try:
        _download_clms_process_api(config, target, token=token, width=width, height=height, year=product_year)
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        target.unlink(missing_ok=True)
        _write_download_manifest(
            output_path,
            {
                "status": "failed",
                "provider": "Copernicus CLMS LCFM LCM-10",
                "collection_id": CLMS_LCM10_COLLECTION_ID,
                "year": product_year,
                "error": str(exc),
            },
        )
        return None
    _write_download_manifest(
        output_path,
        {
            "status": "ready",
            "provider": "Copernicus CLMS LCFM LCM-10",
            "collection_id": CLMS_LCM10_COLLECTION_ID,
            "year": product_year,
            "downloaded": str(target),
            "width": width,
            "height": height,
        },
    )
    return target


def summarize_satellite_land_cover(
    config: MonitorConfig,
    source_path: str | None,
    resources: dict[str, object] | None,
) -> dict[str, object]:
    if not source_path:
        return asdict(_unavailable_summary(resources, "no classified satellite land-cover input was provided"))
    path = Path(source_path)
    if not path.exists():
        return asdict(
            _unavailable_summary(
                resources,
                f"classified land-cover input not found: {path}",
                status="missing_file",
            )
        )
    try:
        if path.is_dir():
            summary = _from_directory(config, path, resources)
        elif path.suffix.lower() in {".tif", ".tiff"}:
            summary = _from_geotiff(config, path, resources)
        elif path.suffix.lower() in {".geojson", ".json"}:
            summary = _from_geojson(path, resources)
        elif path.suffix.lower() == ".csv":
            summary = _from_csv(path, resources)
        else:
            summary = _unavailable_summary(resources, f"unsupported land-cover file type: {path.suffix}", status="unsupported_input")
    except Exception as exc:
        summary = _unavailable_summary(resources, f"land-cover processing error: {exc}", status="processing_error")
    return asdict(summary)


def land_cover_cache_info(config: MonitorConfig, cache_dir: str | Path = LAND_COVER_CACHE_DIR) -> dict[str, object]:
    area_dir = _area_cache_dir(config, Path(cache_dir))
    manifest_path = area_dir / "download_manifest.json"
    files = sorted(
        [path for path in area_dir.glob("*") if _is_auto_land_cover_input(path)],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return {
        "area_cache_dir": str(area_dir),
        "available_files": [str(path) for path in files],
        "latest_file": str(files[0]) if files else None,
        "download_manifest": str(manifest_path) if manifest_path.exists() else None,
        "download_manifest_payload": _read_manifest(manifest_path),
    }


def _from_geotiff(
    config: MonitorConfig,
    path: Path,
    resources: dict[str, object] | None,
) -> SatelliteLandCoverSummary:
    try:
        class_area, crop_area = _geotiff_area(config, path)
    except ImportError:
        return _unavailable_summary(
            resources,
            "GeoTIFF land-cover processing needs optional dependency rasterio",
            source_path=path,
            status="dependency_missing",
        )
    return _summary_from_area(
        class_area,
        crop_area,
        basis="satellite_classified_raster",
        source={"provider": _provider_for_path(path), "path": str(path), "status": "ready"},
        fallback=_resource_fallback(resources),
    )


def _from_directory(
    config: MonitorConfig,
    path: Path,
    resources: dict[str, object] | None,
) -> SatelliteLandCoverSummary:
    class_area: dict[str, float] = {}
    crop_area: dict[str, float] = {}
    files = sorted(file for file in path.glob("*") if _is_land_cover_input(file))
    raster_files = [file for file in files if file.suffix.lower() in {".tif", ".tiff"}]
    if raster_files:
        try:
            for file in raster_files:
                file_class_area, file_crop_area = _geotiff_area(config, file)
                _add_area(class_area, file_class_area)
                _add_area(crop_area, file_crop_area)
        except ImportError:
            return _unavailable_summary(
                resources,
                "GeoTIFF land-cover processing needs optional dependency rasterio",
                source_path=path,
                status="dependency_missing",
            )
        return _summary_from_area(
            class_area,
            crop_area,
            basis="satellite_classified_raster_tiles",
            source={
                "provider": _directory_provider(raster_files),
                "path": str(path),
                "files": [str(file) for file in raster_files],
                "status": "ready",
            },
            fallback=_resource_fallback(resources),
        )
    latest = _latest_land_cover_file(path)
    if latest is None:
        return _unavailable_summary(resources, "land-cover cache is empty", source_path=path, status="empty")
    if latest.suffix.lower() in {".geojson", ".json"}:
        return _from_geojson(latest, resources)
    if latest.suffix.lower() == ".csv":
        return _from_csv(latest, resources)
    return _from_geotiff(config, latest, resources)


def _geotiff_area(config: MonitorConfig, path: Path) -> tuple[dict[str, float], dict[str, float]]:
    try:
        import rasterio
        from rasterio.mask import mask
    except ImportError:
        raise
    geometry = {"type": "Polygon", "coordinates": [config.polygon]}
    with rasterio.open(path) as dataset:
        try:
            data, transform = mask(dataset, [geometry], crop=True, filled=False)
        except ValueError:
            return {}, {}
        values = data[0]
        pixel_area_ha = abs(transform.a * transform.e) / 10000.0
        if dataset.crs and dataset.crs.is_geographic:
            west, south, east, north = bbox(config.polygon)
            mean_lat = (south + north) / 2
            pixel_area_ha = abs(transform.a * 111320.0 * transform.e * 110570.0) / 10000.0
            pixel_area_ha *= max(0.2, __import__("math").cos(__import__("math").radians(mean_lat)))
    class_area: dict[str, float] = {}
    crop_area: dict[str, float] = {}
    for value in values.compressed():
        normalized = _normalize_raster_class(value.item(), path)
        if normalized is None:
            continue
        class_area[normalized] = class_area.get(normalized, 0.0) + pixel_area_ha
    return class_area, crop_area


def _from_geojson(path: Path, resources: dict[str, object] | None) -> SatelliteLandCoverSummary:
    payload = json.loads(path.read_text(encoding="utf-8"))
    features = payload.get("features", []) if isinstance(payload, dict) else []
    class_area: dict[str, float] = {}
    crop_area: dict[str, float] = {}
    for feature in features:
        if not isinstance(feature, dict):
            continue
        properties = feature.get("properties", {})
        if not isinstance(properties, dict):
            continue
        area_ha = _feature_area_ha(feature, properties)
        class_name = _normalize_class(_first_present(properties, ["class", "land_cover", "label", "type", "value"]))
        if class_name and area_ha > 0:
            class_area[class_name] = class_area.get(class_name, 0.0) + area_ha
        crop_name = _crop_name(_first_present(properties, ["crop", "crop_type", "culture", "recolta"]))
        if crop_name and area_ha > 0:
            crop_area[crop_name] = crop_area.get(crop_name, 0.0) + area_ha
    return _summary_from_area(
        class_area,
        crop_area,
        basis="satellite_classified_vector",
        source={"provider": "classified vector", "path": str(path), "status": "ready"},
        fallback=_resource_fallback(resources),
    )


def _from_csv(path: Path, resources: dict[str, object] | None) -> SatelliteLandCoverSummary:
    class_area: dict[str, float] = {}
    crop_area: dict[str, float] = {}
    class_percent: dict[str, float] = {}
    crop_percent: dict[str, float] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            row = {_clean_key(key): value for key, value in row.items()}
            area = _float(row.get("area_ha") or row.get("hectares"))
            percent = _float(row.get("percent") or row.get("percentage"))
            class_name = _normalize_class(row.get("class") or row.get("land_cover") or row.get("label"))
            crop_name = _crop_name(row.get("crop") or row.get("crop_type") or row.get("culture") or row.get("recolta"))
            if class_name and area is not None:
                class_area[class_name] = class_area.get(class_name, 0.0) + area
            elif class_name and percent is not None:
                class_percent[class_name] = class_percent.get(class_name, 0.0) + percent
            if crop_name and area is not None:
                crop_area[crop_name] = crop_area.get(crop_name, 0.0) + area
            elif crop_name and percent is not None:
                crop_percent[crop_name] = crop_percent.get(crop_name, 0.0) + percent
    if class_area or crop_area:
        return _summary_from_area(
            class_area,
            crop_area,
            basis="satellite_classified_table_area",
            source={"provider": "classified table", "path": str(path), "status": "ready"},
            fallback=_resource_fallback(resources),
        )
    return SatelliteLandCoverSummary(
        available=bool(class_percent or crop_percent),
        basis="satellite_classified_table_percent",
        source={"provider": "classified table", "path": str(path), "status": "ready"},
        percentages=_complete_percentages(class_percent),
        hectares=_empty_hectares(),
        crop_percentages={key: round(value, 1) for key, value in crop_percent.items()},
        crop_hectares={},
        crop_detail_available=bool(crop_percent),
        fallback=_resource_fallback(resources),
        notes="Percentages came from a precomputed classified satellite table.",
    )


def _summary_from_area(
    class_area: dict[str, float],
    crop_area: dict[str, float],
    *,
    basis: str,
    source: dict[str, object],
    fallback: dict[str, object],
) -> SatelliteLandCoverSummary:
    total = sum(class_area.values())
    crop_total = sum(crop_area.values())
    return SatelliteLandCoverSummary(
        available=total > 0 or crop_total > 0,
        basis=basis if total > 0 or crop_total > 0 else "not_available",
        source=source if total > 0 or crop_total > 0 else {**source, "status": "empty"},
        percentages=_complete_percentages({key: value * 100 / total for key, value in class_area.items()} if total else {}),
        hectares=_complete_hectares(class_area),
        crop_percentages={key: round(value * 100 / crop_total, 1) for key, value in crop_area.items()} if crop_total else {},
        crop_hectares={key: round(value, 1) for key, value in crop_area.items()},
        crop_detail_available=crop_total > 0,
        fallback=fallback,
        notes="Computed from a classified satellite land-cover input clipped or summarized for the submitted area.",
    )


def _unavailable_summary(
    resources: dict[str, object] | None,
    reason: str,
    *,
    source_path: Path | None = None,
    status: str = "missing_classified_input",
) -> SatelliteLandCoverSummary:
    return SatelliteLandCoverSummary(
        available=False,
        basis="not_available",
        source={
            "provider": "classified satellite land-cover",
            "path": str(source_path) if source_path else None,
            "status": status,
        },
        percentages=_empty_percentages(),
        hectares=_empty_hectares(),
        crop_percentages={},
        crop_hectares={},
        crop_detail_available=False,
        fallback=_resource_fallback(resources),
        notes=reason,
    )


def _resource_fallback(resources: dict[str, object] | None) -> dict[str, object]:
    if not isinstance(resources, dict):
        return {"available": False}
    percentages = resources.get("land_cover_percentages")
    hectares = resources.get("land_cover_hectares")
    if isinstance(percentages, dict):
        return {
            "available": True,
            "basis": resources.get("land_cover_basis", "osm_context"),
            "percentages": percentages,
            "hectares": hectares if isinstance(hectares, dict) else {},
            "note": "Fallback context from OSM, not satellite-classified land cover.",
        }
    total = sum(
        int(resources.get(key, 0) or 0)
        for key in ["urban_landuse_features", "forest_or_green_features", "agriculture_features", "water_body_features"]
    )
    if total <= 0:
        return {"available": False}
    return {
        "available": True,
        "basis": "osm_feature_count_proxy",
        "percentages": {
            "urban_percent": _round_percent(resources.get("urban_landuse_features"), total),
            "forest_percent": _round_percent(resources.get("forest_or_green_features"), total),
            "agriculture_percent": _round_percent(resources.get("agriculture_features"), total),
            "water_percent": _round_percent(resources.get("water_body_features"), total),
        },
        "note": "Fallback context from OSM feature counts, not satellite-classified area.",
    }


def _feature_area_ha(feature: dict[str, object], properties: dict[str, object]) -> float:
    explicit = _float(properties.get("area_ha") or properties.get("hectares"))
    if explicit is not None:
        return explicit
    geometry = feature.get("geometry", {})
    if not isinstance(geometry, dict):
        return 0.0
    if geometry.get("type") == "Polygon":
        rings = geometry.get("coordinates", [])
        return _ring_area_ha(rings[0]) if rings else 0.0
    if geometry.get("type") == "MultiPolygon":
        return sum(_ring_area_ha(poly[0]) for poly in geometry.get("coordinates", []) if poly)
    return 0.0


def _ring_area_ha(points: list[list[float]]) -> float:
    if len(points) < 3:
        return 0.0
    import math

    mean_lat = sum(point[1] for point in points) / len(points)
    scale_x = 111.32 * max(0.2, math.cos(math.radians(mean_lat)))
    scale_y = 110.57
    projected = [(point[0] * scale_x, point[1] * scale_y) for point in points]
    area_km2 = 0.0
    for index, point in enumerate(projected):
        next_point = projected[(index + 1) % len(projected)]
        area_km2 += (point[0] * next_point[1]) - (next_point[0] * point[1])
    return abs(area_km2) * 50.0


def _normalize_class(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return CLMS_LCM10_CODES.get(int(value))
    normalized = str(value).strip().lower().replace("-", "_").replace(" ", "_")
    return LAND_COVER_CLASSES.get(normalized)


def _normalize_raster_class(value: object, path: Path) -> str | None:
    if value is None:
        return None
    if not isinstance(value, (int, float)):
        return _normalize_class(value)
    code = int(value)
    return CLMS_LCM10_CODES.get(code)


def _crop_name(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower().replace(" ", "_")
    return text or None


def _first_present(properties: dict[str, object], keys: list[str]) -> object | None:
    for key in keys:
        if key in properties and properties[key] not in {None, ""}:
            return properties[key]
    return None


def _complete_percentages(values: dict[str, float]) -> dict[str, float | None]:
    result = _empty_percentages()
    for key, value in values.items():
        result[f"{key}_percent"] = round(value, 1)
    return result


def _complete_hectares(values: dict[str, float]) -> dict[str, float | None]:
    result = _empty_hectares()
    for key, value in values.items():
        result[f"{key}_ha"] = round(value, 1)
    return result


def _empty_percentages() -> dict[str, float | None]:
    return {
        "forest_percent": None,
        "urban_percent": None,
        "field_plain_percent": None,
        "agriculture_percent": None,
        "water_percent": None,
        "wetland_percent": None,
    }


def _empty_hectares() -> dict[str, float | None]:
    return {
        "forest_ha": None,
        "urban_ha": None,
        "field_plain_ha": None,
        "agriculture_ha": None,
        "water_ha": None,
        "wetland_ha": None,
    }


def _float(value: object) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _round_percent(value: object, total: int) -> float:
    return round((int(value or 0) * 100) / total, 1) if total else 0.0


def _clean_key(value: object) -> str:
    return str(value or "").lstrip("\ufeff").strip().lower()


def _download_file(url: str, target: Path) -> None:
    request = Request(url, headers={"User-Agent": "cassini-ai-monitor/1.0"})
    with urlopen(request, timeout=180) as response:
        with target.open("wb") as handle:
            shutil.copyfileobj(response, handle)


def _download_clms_process_api(
    config: MonitorConfig,
    target: Path,
    *,
    token: str,
    width: int,
    height: int,
    year: int,
) -> None:
    west, south, east, north = bbox(config.polygon)
    payload = {
        "input": {
            "bounds": {"bbox": [west, south, east, north], "properties": {"crs": "http://www.opengis.net/def/crs/EPSG/0/4326"}},
            "data": [
                {
                    "type": f"byoc-{CLMS_LCM10_COLLECTION_ID}",
                    "dataFilter": {
                        "timeRange": {
                            "from": f"{year}-01-01T00:00:00Z",
                            "to": f"{year}-12-31T23:59:59Z",
                        }
                    },
                }
            ],
        },
        "output": {
            "width": width,
            "height": height,
            "responses": [{"identifier": "default", "format": {"type": "image/tiff"}}],
        },
        "evalscript": """
//VERSION=3
function setup() {
  return {
    input: [{ bands: ["LCM10", "dataMask"] }],
    output: { bands: 1, sampleType: "UINT8", nodataValue: 255 }
  };
}
function evaluatePixel(sample) {
  return [sample.dataMask ? sample.LCM10 : 255];
}
""".strip(),
    }
    request = Request(
        CLMS_PROCESS_API_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "image/tiff",
        },
        method="POST",
    )
    with urlopen(request, timeout=180) as response:
        with target.open("wb") as handle:
            shutil.copyfileobj(response, handle)


def _cdse_access_token() -> str | None:
    client_id = os.environ.get("CDSE_SH_CLIENT_ID") or os.environ.get("SENTINELHUB_CLIENT_ID")
    client_secret = os.environ.get("CDSE_SH_CLIENT_SECRET") or os.environ.get("SENTINELHUB_CLIENT_SECRET")
    if not client_id or not client_secret:
        return None
    body = urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }
    ).encode("utf-8")
    request = Request(CDSE_TOKEN_URL, data=body, headers={"Content-Type": "application/x-www-form-urlencoded"}, method="POST")
    with urlopen(request, timeout=60) as response:
        payload = json.loads(response.read().decode("utf-8"))
    token = payload.get("access_token")
    return str(token) if token else None


def _process_raster_dimensions(config: MonitorConfig, *, max_pixels: int) -> tuple[int, int]:
    west, south, east, north = bbox(config.polygon)
    import math

    mean_lat = (south + north) / 2
    width = max(1, int(math.ceil(abs(east - west) * 111_320 * max(0.2, math.cos(math.radians(mean_lat))) / 10)))
    height = max(1, int(math.ceil(abs(north - south) * 110_570 / 10)))
    if width * height <= max_pixels:
        return width, height
    scale = math.sqrt(max_pixels / (width * height))
    return max(1, int(width * scale)), max(1, int(height * scale))


def _write_download_manifest(output_dir: Path, payload: dict[str, object]) -> None:
    (output_dir / "download_manifest.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _read_manifest(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _add_area(target: dict[str, float], source: dict[str, float]) -> None:
    for key, value in source.items():
        target[key] = target.get(key, 0.0) + value


def _latest_land_cover_file(area_dir: Path) -> Path | None:
    files = [path for path in area_dir.glob("*") if _is_auto_land_cover_input(path)]
    if not files:
        return None
    return max(files, key=lambda path: path.stat().st_mtime)


def _cleanup_old_land_cover(area_dir: Path, *, keep_latest: int) -> None:
    if keep_latest < 1:
        keep_latest = 1
    files = sorted(
        [
            path
            for path in area_dir.glob("*")
            if _is_auto_land_cover_input(path)
        ],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for path in files[keep_latest:]:
        path.unlink(missing_ok=True)


def _copernicus_lcfm_files(area_dir: Path) -> list[Path]:
    return sorted(area_dir.glob("LCFM_LCM-10_*_MAP.tif"))


def _is_land_cover_input(path: Path) -> bool:
    return (
        path.is_file()
        and path.suffix.lower() in SUPPORTED_LAND_COVER_SUFFIXES
        and path.name != "download_manifest.json"
    )


def _is_auto_land_cover_input(path: Path) -> bool:
    return _is_land_cover_input(path) and not _is_legacy_esa_worldcover_path(path)


def _provider_for_path(path: Path) -> str:
    if path.name.startswith("LCFM_LCM-10_"):
        return "Copernicus CLMS LCFM LCM-10 / classified raster"
    return "classified satellite raster"


def _directory_provider(paths: list[Path]) -> str:
    if paths and all(path.name.startswith("LCFM_LCM-10_") for path in paths):
        return "Copernicus CLMS LCFM LCM-10 / classified raster tiles"
    return "classified satellite raster tiles"


def _is_legacy_esa_worldcover_path(path: Path) -> bool:
    return path.name.startswith("ESA_WorldCover_")


def _bbox_slug(config: MonitorConfig) -> str:
    west, south, east, north = bbox(config.polygon)
    return _slug(f"{west:.4f}-{south:.4f}-{east:.4f}-{north:.4f}")


def _area_cache_dir(config: MonitorConfig, cache_dir: Path) -> Path:
    west, south, east, north = bbox(config.polygon)
    slug = _slug(f"{config.area_name}-{west:.4f}-{south:.4f}-{east:.4f}-{north:.4f}")
    return cache_dir / slug


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "selected-area"
