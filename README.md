# 🚛 Fleet Operations Analytics Platform

![Sector](https://img.shields.io/badge/Sector-Fleet%20%2F%20Logistics-5b2b99?style=flat)
![CI](https://img.shields.io/badge/CI-passing-0f7a4b?style=flat&logo=githubactions)
![Python](https://img.shields.io/badge/Python-3.12-blue?style=flat&logo=python)

**[← Back to live portfolio](https://andiswamatai.github.io)**

## 🚀 Overview

A telematics-driven fleet operations analytics platform designed to transform raw vehicle data into actionable operational intelligence.

The system processes GPS telematics, fuel consumption logs, and maintenance records to compute fleet performance KPIs and automatically flag operational risks such as inefficiency, unsafe driving behaviour, and maintenance non-compliance.

It reflects real-world fleet analytics use cases similar to those used in logistics and mobility platforms such as GUUD Mobility.

---

## 🧠 Business Problem

Fleet operators manage large volumes of vehicle data across multiple disconnected systems, including:

- GPS tracking systems (telematics)
- Fuel management systems
- Maintenance scheduling systems

This leads to:

- Poor visibility of vehicle performance
- Delayed detection of maintenance issues
- Increased fuel and operational costs
- Inefficient asset utilisation
- Reactive rather than proactive fleet management

---
## Solutions Overview 

This platform implements an end-to-end fleet analytics pipeline that converts raw operational data into structured performance insights.

The system:

- Ingests telematics, fuel, and maintenance datasets
- Computes per-vehicle operational KPIs
- Detects anomalies and operational inefficiencies
- Generates automated fleet alerts
- Produces visual dashboards for fleet performance monitoring

---
## Architecture

📡 Data Sources
- Vehicle Telematics (GPS data)
- Fuel Consumption Logs
- Maintenance Records

        ↓

🥉 Data Layer (Raw Inputs)
- Telemetry streams
- Fuel transactions
- Maintenance history

        ↓

🥈 Processing Layer
- KPI computation per vehicle
- Data cleaning and aggregation
- Time-based analysis

        ↓

🥇 Analytics Layer
- Fleet performance KPIs
- Operational risk scoring
- Alert generation engine

        ↓

📊 Consumption Layer
- Fleet dashboard (matplotlib / BI equivalent)
- Operational reports
- Maintenance recommendations

## KPIs

| KPI | What it measures | Threshold |
|-----|------------------|----------|
| Fuel efficiency (km/L) | Cost efficiency per vehicle | < 7.0 km/L |
| Harsh driving events / 100 km | Driver behaviour & safety risk | > 1.5 |
| Maintenance compliance | Vehicle servicing adherence | < 80% |
| Utilisation rate | Asset usage efficiency | < 60% |
| Fuel cost per km | Route and cost optimisation | — |

## Tech stack

Python, pandas, matplotlib.

## Engineerign Design

This platform demonstrates key data engineering and analytics patterns:

- Multi-source data ingestion (telemetry + financial + maintenance)
- Time-series aggregation for vehicle behaviour analysis
- Rule-based anomaly detection for operational alerts
- KPI computation per entity (vehicle-level analytics)
- Batch processing pipeline for daily operational reporting

## Arlerting Layer
  
The system automatically generates operational alerts when thresholds are breached:

- Fuel inefficiency detected
- Driver safety violations (harsh braking/acceleration)
- Overdue or incomplete maintenance schedules
- Underutilised fleet assets

Example:

vehicle_id | alert
-----------|-------------------------------------
V005       | MAINTENANCE_OVERDUE
           | Only 66.7% of scheduled jobs completed

Sample alert output:

```
=== Fleet Alerts ===
vehicle_id            alert                                       detail
      V005  MAINTENANCE_OVERDUE  Only 66.7% of scheduled jobs completed
```

## Outputs

The platform produces:

- Vehicle-level KPI dataset (vehicle_kpis.csv)
- Fleet operational alerts (fleet_alerts.csv)
- Fleet performance dashboard (fleet_dashboard.png)
- Maintenance compliance reports

## Business Value 

This system enables fleet operators to:

- Reduce fuel and operational costs
- Improve vehicle utilisation rates
- Proactively identify maintenance risks
- Improve driver safety and compliance
- Shift from reactive to predictive fleet management

## Production Enhancement

If deployed in a production environment:

- Azure Data Factory ingestion from telematics APIs (Geotab / MiX Telematics)
- Storage in Azure Data Lake (Bronze/Silver/Gold architecture)
- Databricks for scalable telemetry processing
- Power BI dashboards for fleet operations teams
- Integration with maintenance systems to auto-create work orders
- Real-time alerting for safety and compliance breaches
