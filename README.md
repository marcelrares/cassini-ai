# cassini-ai

Cassini AI is a Python calamity monitoring starter for a configurable area. It builds a site-ready dashboard data bundle with weather-driven warning indices for flood, drought, storm or strong wind, heatwave, and wildfire exposure.

The scores are warning indices, not official alerts and not event probabilities. Use them as decision-support signals that should be calibrated against local historical events and official warning criteria before production use.

## What It Does

- Loads the monitored area and thresholds from `config/monitor_config.json`.
- Reads weather from Open-Meteo by default.
- Can run fully offline with bundled demo weather values.
- Optionally checks Copernicus Data Space STAC products for satellite evidence.
- Optionally loads historical weather, seasonal baselines, and elevation context.
- Optionally reads classified satellite land-cover exports for forest, urban, fields, agriculture, water, wetlands, and crop percentages.
- Adjusts risk thresholds using broad regional calibration from the submitted bbox.
- Summarizes sensor health from `data/sensors.csv`.
- Writes the frontend data bundle under `out/site/`.

## Project Layout

```text
.
|-- hourly_monitor.py              # CLI entry point
|-- calamity_ai/                   # Monitoring package
|-- config/monitor_config.json     # Area, thresholds, and providers
|-- data/sensors.csv               # Example sensor inventory
|-- data/resources/                # Ignored OSM context cache per monitored bbox
|-- data/land_cover/               # Ignored Copernicus CLMS / classified land-cover cache
|-- requirements.txt               # Optional Python dependencies
`-- .gitignore                     # Local Python/output ignores
```

## Requirements

- Python 3.10 or newer.
- Internet access for the normal Open-Meteo, Copernicus, elevation, and resource refresh paths.

Install optional dependencies:

```powershell
python -m pip install -r requirements.txt
```

## Quick Start

Run the offline demo first:

```powershell
python hourly_monitor.py --demo --no-context --no-copernicus --no-resources --print-json
```

Run the default online monitor:

```powershell
python hourly_monitor.py
```

Site-ready output is written under `out/site/`.

## Common Commands

Skip satellite checks:

```powershell
python hourly_monitor.py --no-copernicus
```

Skip historical weather and elevation context:

```powershell
python hourly_monitor.py --no-context
```

Refresh cached OpenStreetMap boundary and context data:

```powershell
python hourly_monitor.py --update-resources
```

Run a submitted bbox, for example a region in Turkey:

```powershell
python hourly_monitor.py --bbox 29 39 30 40 --area-name turkey-test --update-resources
```

Add classified satellite land-cover/crop data to the final dashboard section:

```powershell
python hourly_monitor.py --bbox 29 39 30 40 --area-name turkey-test --land-cover data/landcover/turkey_region.csv
```

If `--land-cover` is omitted, the monitor auto-selects the newest Copernicus/classified file from the per-bbox cache. If the cache is empty, it requests a Copernicus CLMS LCFM LCM-10 raster for the submitted bbox through Copernicus Data Space Sentinel Hub Process API:

```text
data/land_cover/<area-name-and-bbox>/
```

Automatic CLMS download needs Sentinel Hub/CDSE client credentials in the environment:

```powershell
$env:CDSE_SH_CLIENT_ID="..."
$env:CDSE_SH_CLIENT_SECRET="..."
```

Old cached classified files are removed automatically. Keep a different number with:

```powershell
python hourly_monitor.py --bbox 29 39 30 40 --area-name turkey-test --land-cover-keep-latest 3
```

Disable automatic Copernicus CLMS download, change the output pixel cap, or request a specific LCFM reference year:

```powershell
python hourly_monitor.py --bbox 29 39 30 40 --area-name turkey-test --no-land-cover-download
python hourly_monitor.py --bbox 29 39 30 40 --area-name turkey-test --land-cover-max-pixels 8000000
python hourly_monitor.py --bbox 29 39 30 40 --area-name turkey-test --land-cover-year 2026
```

Print the full report to the console:

```powershell
python hourly_monitor.py --print-json
```

## Configuration

Edit `config/monitor_config.json` to change:

- `area`: monitored polygon and display name.
- `weather`: forecast window.
- `thresholds`: risk model reference thresholds.
- `sensor_health`: minimum online ratio and stale sensor age.
- `context`: historical lookback and baseline years.
- `copernicus`: STAC URL, lookback period, limits, and auxiliary collections.

The configured thresholds are treated as defaults. At runtime, the monitor applies broad regional calibration from the bbox center, for example Mediterranean, continental Europe, northern Europe, Middle East dry, tropical, or global default.

## Data Inputs

`data/sensors.csv` expects:

```csv
sensor_id,type,latitude,longitude,status,last_seen_utc
```

`last_seen_utc` can be an ISO timestamp ending in `Z` or `NOW` for demos.

Weather is always fetched from Open-Meteo for the configured polygon or runtime `--bbox`. Use `--demo` only for offline smoke tests.

`--land-cover` accepts a classified satellite export:

- `.tif` / `.tiff`: classified raster, if optional `rasterio` is installed.
- `.geojson` / `.json`: classified polygons with `class`/`land_cover` and optional `crop`/`crop_type`.
- `.csv`: rows with `class`, optional `crop`, and either `area_ha` or `percent`.

Supported broad classes include `forest`, `urban`, `grassland`/`field_plain`, `agriculture`, `water`, and `wetland`. Copernicus CLMS LCFM LCM-10 numeric raster classes are mapped for raster inputs.

For broad land-cover classes, the monitor can request Copernicus CLMS LCFM LCM-10 automatically when CDSE/Sentinel Hub credentials are configured. Crop type percentages still require a crop-specific classification layer or table, because global land-cover maps identify cropland but not individual crops such as wheat or corn.

## Outputs

Each run writes `out/site/manifest.json`, the frontend entrypoint that links the dashboard payload and map-ready layers.

The `out/` directory is ignored by git.

## Site Data Pipeline

The monitor also writes a site-ready data bundle under `out/site/`.

```text
sources -> standardized -> processing -> manifest -> site
```

Frontend code should load only:

```text
out/site/manifest.json
```

The manifest points to:

- `dashboard.json`: compact payload for dashboard cards, risk overview, sensor status, weather widgets, alerts, prediction panels, and map component links.
- `standardized/satellite.stac.json`: curated satellite evidence in STAC-like JSON.
- `standardized/weather.coverage.json`: weather values in a CoverageJSON-style structure.
- `standardized/maps.geojson`: monitored area and map context.
- `standardized/events.geojson`: elevated risks and system events for map icons.
- `processing/predictions.geojson`: risk predictions ready for map coloring.

Raw inputs are kept separately under `out/site/sources/` so the frontend does not need to understand provider-specific payloads.

## Scheduling

Linux cron example:

```cron
0 * * * * /usr/bin/python3 /path/to/cassini-ai/hourly_monitor.py
```

Windows Task Scheduler action:

```powershell
python C:\path\to\cassini-ai\hourly_monitor.py
```

## Notes For Production

- Calibrate thresholds with local historical events.
- Validate model output against official weather and emergency alerts.
- Replace demo sensors with real sensor feeds.
- Keep cached resources fresh with `--update-resources` when the monitored area changes.
- For bbox requests, OSM resources are cached per area+bbox. Very large bboxes are marked as skipped for OSM instead of making one oversized Overpass request.
- Crop percentages require a classified crop layer or table; Sentinel/STAC metadata alone is not enough to infer exact crop type percentages.
- Treat risk values as decision-support indices, not official hazard polygons.
