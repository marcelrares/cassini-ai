# AI Calamity Monitor

Python starter for hourly weather, satellite, and sensor risk monitoring:

- flood risk (`flood`)
- drought risk (`drought`)
- storm / strong wind risk (`storm`)
- heatwave risk (`heatwave`)
- wildfire risk (`wildfire`)
- Copernicus Sentinel-1 and Sentinel-2 products over the monitored area
- additional Copernicus/CLMS/MODIS evidence collections where available
- historical weather context for the last 30 days
- historical weather context for the last 90 days
- same-season baseline from up to 10 previous years for drought, flood, and wildfire calibration
- elevation / terrain context
- local AOI sector ranking for likely flood, drought, and wildfire exposure
- multi-day forward prediction indices
- local resource cache from OpenStreetMap/Nominatim and Overpass
- sensor health from CSV
- JSONL and CSV export

## Normal Run, No Google Account

```powershell
python hourly_monitor.py
```

By default, the script loads real forecast data from Open-Meteo for the polygon in
`config/monitor_config.json`, searches Copernicus Data Space STAC for satellite
products over the same area, and adds historical weather plus elevation context.
It does not require Google Cloud, API keys, or manual account configuration.
Reports are written to `out/reports.jsonl` and `out/reports.csv`.

Flood, drought, and wildfire scores are not based only on the next 24 hours.
They are calibrated with:

- the last 7 and 30 days of rainfall
- the last 90 days of rainfall
- dry-day count over the last 30 days
- dry-day count over the last 90 days
- 30-day evapotranspiration
- 90-day evapotranspiration
- the same calendar-season window from up to the previous 10 years
- elevation range / terrain class for runoff interpretation

The report also includes `zone_analysis`, a 3x3 grid over the monitored AOI.
This ranks relative local exposure sectors, for example the sectors most likely
to be affected by flood because they are lower elevation and in a steep-terrain
AOI. These are not probabilities and not official warning polygons. The value
means "more or less exposed than the other monitored sectors if the hazard
materializes", not "chance that this sector will flood".

The `predictions` block gives the next few days of warning-index forecasts using
the same scoring engine and historical baseline.

Default data sources:

- Open-Meteo Forecast API for current forecast and fire-weather variables
- Open-Meteo Historical Weather API for recent past conditions
- Open-Meteo Elevation API for terrain context
- Copernicus Data Space STAC API for Sentinel-1 GRD and Sentinel-2 L2A
- OpenStreetMap/Nominatim for the cached Iași boundary
- OpenStreetMap/Overpass for cached hydrology and land-cover context

Refresh local boundary and OSM context resources:

```powershell
python hourly_monitor.py --update-resources
```

The cache is stored in `data/resources/` and includes waterways, water bodies,
urban/impervious proxies, vegetation/forest, and agriculture features. These
improve the relative zone rankings. They are still not official hazard maps.

To skip Copernicus:

```powershell
python hourly_monitor.py --no-copernicus
```

To skip historical/elevation context:

```powershell
python hourly_monitor.py --no-context
```

## Offline Demo

```powershell
python hourly_monitor.py --demo
```

The demo uses `data/weather_features.json`.

## Optional Earth Engine Run

Earth Engine remains optional. Google requires a project id for quota and authentication,
so this mode cannot run with login only:

```powershell
python hourly_monitor.py --provider earthengine --project ID_PROIECT_GOOGLE
```

Or save the project into config:

```powershell
python hourly_monitor.py --provider earthengine --project ID_PROIECT_GOOGLE --save-project
```

## Hourly Scheduling

Linux/VM:

```cron
0 * * * * /usr/bin/python3 /path/hourly_monitor.py >> /path/monitor.log 2>&1
```

Windows Task Scheduler: create an hourly task that runs:

```powershell
python C:\path\to\hourly_monitor.py
```

## Extension Points

- edit the polygon in `config/monitor_config.json`
- add real sensors in `data/sensors.csv`
- tune thresholds in `thresholds`
- add new risk models in `calamity_ai/scoring.py`

The initial thresholds are heuristic. For production, calibrate them with local
historical events and official warning criteria.
