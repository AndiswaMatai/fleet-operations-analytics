# Fleet Operations Analytics

A telematics and operational analytics pipeline for fleet management — computes KPIs for fuel efficiency, vehicle utilisation, driver behaviour, and maintenance compliance, then surfaces vehicles needing attention through an automated alert layer.

This reflects the kind of work done at GUUD Mobility: analysing large telematics datasets to identify cost reduction opportunities and operational risks.

## Architecture

```
data/raw/
  telematics.csv     ─┐
  fuel_logs.csv       ├─▶ analyse.py ─▶ vehicle_kpis.csv
  maintenance.csv    ─┘                  fleet_alerts.csv
                                               │
                                          report.py
                                               │
                                     fleet_dashboard.png
```

| Module | What it does |
|---|---|
| `src/generate_sample_data.py` | Synthesises 30 days of GPS telematics, fuel fill-ups, and maintenance records for 5 vehicles |
| `src/analyse.py` | Computes per-vehicle KPIs; flags alerts when vehicles breach thresholds |
| `src/report.py` | Renders a 3-panel dashboard chart (fuel efficiency, harsh events, maintenance compliance) |

## KPIs

| KPI | What it measures | Alert threshold |
|---|---|---|
| Fuel efficiency (km/L) | Cost control | < 7.0 km/L |
| Harsh events / 100 km | Driver behaviour & safety | > 1.5 events |
| Maintenance compliance | Operational readiness | < 80% |
| Utilisation rate | Asset efficiency | < 60% of available days |
| Fuel cost per km | Route/vehicle cost comparison | — |

## Tech stack

Python, pandas, matplotlib.

## Running it

```bash
pip install -r requirements.txt
python src/generate_sample_data.py
python src/analyse.py
python src/report.py
```

Sample alert output:

```
=== Fleet Alerts ===
vehicle_id            alert                                       detail
      V005  MAINTENANCE_OVERDUE  Only 66.7% of scheduled jobs completed
```

Run the tests:

```bash
python -m unittest discover -s tests -v
```

## What I'd add for production

- Connect directly to the telematics provider's REST API (Geotab, MiX Telematics) via an Azure Data Factory pipeline for daily automated ingestion.
- Publish KPIs and alerts to a Power BI dashboard with drill-through to individual vehicle trip history.
- Integrate with a maintenance scheduling system to auto-raise work orders when compliance drops below threshold.

## License

MIT
