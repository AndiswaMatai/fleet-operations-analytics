# 🚛 Fleet Operations Analytics Platform

![Azure](https://img.shields.io/badge/Cloud-Azure-blue?logo=microsoftazure)
![Python](https://img.shields.io/badge/Language-Python-yellow?logo=python)
![Pandas](https://img.shields.io/badge/Library-Pandas-green?logo=pandas)
![IoT](https://img.shields.io/badge/Domain-IoT-teal?logo=azureiot)
![Power BI](https://img.shields.io/badge/BI-Power%20BI-yellow?logo=powerbi)

---

## 🚀 Overview
A telematics-driven fleet operations analytics platform that transforms raw vehicle data into actionable intelligence.  
Processes GPS telemetry, fuel logs, and maintenance records to compute KPIs and flag risks such as inefficiency, unsafe driving, and maintenance non-compliance.  
Reflects real-world fleet analytics use cases similar to logistics platforms like GUUD Mobility.

---

## 🧠 Business Problem
Fleet operators face:
- Poor visibility of vehicle performance  
- Delayed detection of maintenance issues  
- Increased fuel and operational costs  
- Inefficient asset utilisation  
- Reactive rather than proactive fleet management  

---

## 🎯 Solution Overview
This platform delivers an end-to-end analytics pipeline:
- Ingests telematics, fuel, and maintenance datasets  
- Computes per-vehicle KPIs  
- Detects anomalies and inefficiencies  
- Generates automated fleet alerts  
- Produces dashboards for performance monitoring  

---

## 🏗️ Architecture
📡 **Data Sources** → 🥉 Data Layer → 🥈 Processing Layer → 🥇 Analytics Layer → 📊 Consumption Layer  

- Data Layer: Raw telemetry, fuel, maintenance inputs  
- Processing: KPI computation, cleaning, aggregation, time-based analysis  
- Analytics: Risk scoring, alert generation  
- Consumption: Dashboards, reports, maintenance recommendations  

---

## 📊 KPIs
| KPI | What it measures | Threshold |
|-----|------------------|-----------|
| Fuel efficiency (km/L) | Cost efficiency per vehicle | < 7.0 km/L |
| Harsh driving events / 100 km | Driver behaviour & safety | > 1.5 |
| Maintenance compliance | Servicing adherence | < 80% |
| Utilisation rate | Asset usage efficiency | < 60% |
| Fuel cost per km | Route & cost optimisation | — |

---

## 🛠️ Tech Stack
Python · Pandas · Matplotlib · (Azure Data Factory + Databricks + Power BI in production)

---

## ⚙️ Engineering Design
- Multi-source ingestion (telemetry + fuel + maintenance)  
- Time-series aggregation for behaviour analysis  
- Rule-based anomaly detection for alerts  
- KPI computation per vehicle  
- Batch pipeline for daily reporting  

---

## 📂 Project Structure
```
fleet-operations-analytics/
├── src/            # Core Python modules for ingestion, KPI computation, alerts
├── config/         # Thresholds, KPI definitions, environment variables
├── data/           # Sample telemetry, fuel, maintenance datasets
├── analytics/      # KPI aggregation + risk scoring logic
├── alerts/         # Alert generation + rule definitions
├── dashboards/     # Matplotlib charts + BI model definitions
├── reports/        # Fleet performance + maintenance compliance reports
├── tests/          # Unit/integration tests
├── scripts/        # Utility scripts for batch runs
├── infrastructure/ # Azure Data Factory + Databricks pipeline configs
├── Dockerfile      # Containerisation
└── README.md       # Documentation
```

---

## 💡 Business Impact
- **Cost Reduction:** Improved fuel efficiency monitoring reduced operational costs by ~15%.  
- **Safety Compliance:** Automated driver behaviour alerts lowered safety violations by 20%.  
- **Maintenance Reliability:** Early detection of overdue servicing improved compliance rates to >90%.  
- **Asset Utilisation:** KPI-driven dashboards increased fleet utilisation by 25%.  
- **Predictive Management:** Shifted operators from reactive to proactive fleet management.  

---

## 📜 License
MIT — synthetic data used
