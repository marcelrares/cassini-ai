# cassini-ai

Cassini AI is a Python calamity monitoring starter for the Iasi area. It builds a site-ready dashboard data bundle with weather-driven warning indices for flood, drought, storm or strong wind, heatwave, and wildfire exposure.

The scores are warning indices, not official alerts and not event probabilities. Use them as decision-support signals that should be calibrated against local historical events and official warning criteria before production use.

## What It Does

- Loads the monitored area and thresholds from `config/monitor_config.json`.
- Reads weather from Open-Meteo by default.
- Can run fully offline with bundled demo weather values.
- Optionally checks Copernicus Data Space STAC products for satellite evidence.
- Optionally loads historical weather, seasonal baselines, and elevation context.
- Summarizes sensor health from `data/sensors.csv`.
- Writes the frontend data bundle under `out/site/`.

## Project Layout

```text
.
|-- hourly_monitor.py              # CLI entry point
|-- calamity_ai/                   # Monitoring package
|-- config/monitor_config.json     # Area, thresholds, and providers
|-- data/sensors.csv               # Example sensor inventory
|-- data/weather_features.json     # Offline local weather sample
|-- data/resources/                # Cached OSM/Nominatim context
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

Use local weather from `data/weather_features.json`:

```powershell
python hourly_monitor.py --provider local --weather data/weather_features.json
```

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

## Data Inputs

`data/sensors.csv` expects:

```csv
sensor_id,type,latitude,longitude,status,last_seen_utc
```

`last_seen_utc` can be an ISO timestamp ending in `Z` or `NOW` for demos.

`data/weather_features.json` is used with `--provider local` and includes precipitation, temperature, wind, CAPE, soil moisture, humidity, evapotranspiration, and vapor pressure deficit fields.

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
- Treat risk values as decision-support indices, not official hazard polygons.
