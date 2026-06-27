"""
fleet-operations-analytics/analytics/risk_scoring.py
KPI aggregation, time-series analysis, and risk scoring logic.
"""

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import DataFrame


# ── Daily KPI Aggregation ────────────────────────────────────────────────────

def aggregate_daily_telemetry(telemetry_df: DataFrame) -> DataFrame:
    """
    Collapses raw telemetry events into daily per-vehicle summaries.
    Used as the basis for trend and rolling-window analysis.
    """
    return (
        telemetry_df
        .groupBy("vehicle_id", "event_date")
        .agg(
            F.sum("distance_km").alias("daily_distance_km"),
            F.sum("harsh_braking").alias("daily_harsh_braking"),
            F.sum("harsh_accel").alias("daily_harsh_accel"),
            F.sum("idling_minutes").alias("daily_idling_minutes"),
            F.avg("speed_kmh").alias("avg_speed_kmh"),
            F.max("speed_kmh").alias("max_speed_kmh"),
            F.count("event_id").alias("event_count"),
        )
        .withColumn(
            "daily_harsh_events",
            F.col("daily_harsh_braking") + F.col("daily_harsh_accel")
        )
    )


def aggregate_daily_fuel(fuel_df: DataFrame) -> DataFrame:
    """
    Collapses fuel fill records to a daily per-vehicle summary.
    """
    return (
        fuel_df
        .groupBy("vehicle_id", "fill_date")
        .agg(
            F.sum("litres_filled").alias("daily_litres"),
            F.sum("cost_zar").alias("daily_fuel_cost_zar"),
            F.count("fuel_id").alias("fill_count"),
        )
    )


# ── 7-Day Rolling Window Analysis ────────────────────────────────────────────

def compute_rolling_kpis(daily_telemetry_df: DataFrame) -> DataFrame:
    """
    7-day rolling window per vehicle to detect worsening trends.
    Includes rolling harsh events and rolling distance.
    """
    window_7d = (
        Window
        .partitionBy("vehicle_id")
        .orderBy(F.col("event_date").cast("long"))
        .rangeBetween(-6 * 86400, 0)          # 7 days in seconds
    )

    return (
        daily_telemetry_df
        .withColumn("rolling_7d_distance_km",     F.sum("daily_distance_km").over(window_7d))
        .withColumn("rolling_7d_harsh_events",    F.sum("daily_harsh_events").over(window_7d))
        .withColumn("rolling_7d_idling_minutes",  F.sum("daily_idling_minutes").over(window_7d))
        .withColumn(
            "rolling_7d_harsh_per_100km",
            F.round(
                (F.col("rolling_7d_harsh_events") / F.col("rolling_7d_distance_km")) * 100, 2
            )
        )
        .withColumn(
            "behaviour_trend_flag",
            F.when(F.col("rolling_7d_harsh_per_100km") > 1.5, F.lit(1))
             .otherwise(F.lit(0))
        )
    )


# ── Vehicle-Level Risk Scoring ────────────────────────────────────────────────

def score_vehicle_risk(master_kpi_df: DataFrame) -> DataFrame:
    """
    Enriches the master KPI table with a weighted risk score.

    Weights:
        HIGH_RISK_VEHICLE (risk_score >= 3) : 4 pts
        HARSH_DRIVING_FLAG                  : 3 pts
        MAINTENANCE_FLAG                    : 2 pts
        FUEL_EFFICIENCY_FLAG                : 2 pts
        UTILISATION_FLAG                    : 1 pt
    """
    return (
        master_kpi_df
        .withColumn(
            "weighted_risk_score",
            (F.col("harsh_driving_flag")    * 3)
            + (F.col("maintenance_flag")    * 2)
            + (F.col("fuel_efficiency_flag")* 2)
            + (F.col("utilisation_flag")    * 1)
        )
        .withColumn(
            "risk_category",
            F.when(F.col("weighted_risk_score") >= 6, F.lit("CRITICAL"))
             .when(F.col("weighted_risk_score").between(4, 5), F.lit("HIGH"))
             .when(F.col("weighted_risk_score").between(2, 3), F.lit("MEDIUM"))
             .otherwise(F.lit("LOW"))
        )
        .withColumn(
            "priority_rank",
            F.rank().over(
                Window.orderBy(F.col("weighted_risk_score").desc())
            )
        )
    )


# ── Fleet-Level Summary ───────────────────────────────────────────────────────

def build_fleet_summary(scored_kpi_df: DataFrame) -> DataFrame:
    """
    Single-row fleet health summary. Intended for executive dashboards.
    """
    return (
        scored_kpi_df
        .agg(
            F.count("vehicle_id").alias("total_vehicles"),
            F.avg("fuel_efficiency_km_l").alias("fleet_avg_fuel_efficiency"),
            F.avg("harsh_events_per_100km").alias("fleet_avg_harsh_events_per_100km"),
            F.avg("compliance_rate").alias("fleet_avg_compliance_rate"),
            F.avg("utilisation_rate").alias("fleet_avg_utilisation_rate"),
            F.sum("fuel_efficiency_flag").alias("vehicles_low_fuel_efficiency"),
            F.sum("harsh_driving_flag").alias("vehicles_harsh_driving"),
            F.sum("maintenance_flag").alias("vehicles_maintenance_noncompliant"),
            F.sum("utilisation_flag").alias("vehicles_underutilised"),
            F.sum(F.when(F.col("risk_category") == "CRITICAL", 1).otherwise(0))
             .alias("vehicles_critical_risk"),
        )
        .withColumn(
            "fleet_health_score",
            F.round(
                (
                    F.col("fleet_avg_fuel_efficiency") / 7.0
                    + F.col("fleet_avg_compliance_rate")
                    + F.col("fleet_avg_utilisation_rate")
                ) / 3 * 100,
                1
            )
        )
        .withColumn("snapshot_timestamp", F.current_timestamp())
    )


# ── Driver-Level Behaviour Ranking ────────────────────────────────────────────

def rank_drivers_by_behaviour(telemetry_df: DataFrame) -> DataFrame:
    """
    Ranks drivers by harsh events per 100 km (worst to best).
    Useful for coaching and incentive programmes.
    """
    driver_stats = (
        telemetry_df
        .filter(F.col("driver_id").isNotNull())
        .groupBy("driver_id")
        .agg(
            F.sum("distance_km").alias("total_distance_km"),
            (F.sum("harsh_braking") + F.sum("harsh_accel")).alias("total_harsh_events"),
            F.avg("speed_kmh").alias("avg_speed_kmh"),
            F.sum("idling_minutes").alias("total_idling_minutes"),
        )
        .withColumn(
            "harsh_events_per_100km",
            F.round(
                (F.col("total_harsh_events") / F.col("total_distance_km")) * 100, 2
            )
        )
        .withColumn(
            "driver_safety_rank",
            F.rank().over(Window.orderBy(F.col("harsh_events_per_100km").asc()))
        )
        .withColumn(
            "driver_risk_flag",
            F.when(F.col("harsh_events_per_100km") > 1.5, F.lit(1)).otherwise(F.lit(0))
        )
    )
    return driver_stats
