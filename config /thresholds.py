"""
fleet-operations-analytics/config/thresholds.py
Central KPI threshold definitions and environment configuration.
"""

# ── KPI Thresholds ───────────────────────────────────────────────────────────

KPI_THRESHOLDS = {
    # Fuel
    "fuel_efficiency_min_km_l":         7.0,
    "fuel_cost_per_km_warning_zar":     3.50,

    # Driver behaviour
    "harsh_events_per_100km_max":       1.5,
    "speed_limit_kmh":                  120.0,
    "idling_minutes_daily_max":         60.0,

    # Maintenance
    "maintenance_compliance_min_pct":   0.80,
    "service_due_soon_days":            7,

    # Utilisation
    "utilisation_min_rate":             0.60,

    # Risk scoring
    "risk_score_critical_threshold":    3,
}

# ── Observation Window ───────────────────────────────────────────────────────

OBSERVATION_WINDOW_DAYS = 30

# ── Input Data Paths (override with env vars in production) ──────────────────

DATA_PATHS = {
    "telemetry":    "abfss://raw@<storage>.dfs.core.windows.net/fleet/telemetry/",
    "fuel":         "abfss://raw@<storage>.dfs.core.windows.net/fleet/fuel/",
    "maintenance":  "abfss://raw@<storage>.dfs.core.windows.net/fleet/maintenance/",
}

OUTPUT_PATHS = {
    "kpi_master":       "abfss://curated@<storage>.dfs.core.windows.net/fleet/kpi_master/",
    "alerts":           "abfss://curated@<storage>.dfs.core.windows.net/fleet/alerts/",
    "fleet_summary":    "abfss://curated@<storage>.dfs.core.windows.net/fleet/fleet_summary/",
    "driver_ranking":   "abfss://curated@<storage>.dfs.core.windows.net/fleet/driver_ranking/",
    "daily_telemetry":  "abfss://curated@<storage>.dfs.core.windows.net/fleet/daily_telemetry/",
}

# ── Spark Configuration ──────────────────────────────────────────────────────

SPARK_CONFIG = {
    "spark.sql.shuffle.partitions":                 "200",
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled":   "true",
    "spark.sql.extensions":                         "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog":              "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}

# ── Delta Table Names (Unity Catalog) ───────────────────────────────────────

DELTA_TABLES = {
    "telemetry_raw":    "fleet_catalog.raw.telemetry",
    "fuel_raw":         "fleet_catalog.raw.fuel",
    "maintenance_raw":  "fleet_catalog.raw.maintenance",
    "kpi_master":       "fleet_catalog.curated.kpi_master",
    "alerts":           "fleet_catalog.curated.alerts",
    "fleet_summary":    "fleet_catalog.curated.fleet_summary",
}
