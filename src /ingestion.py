"""
fleet-operations-analytics/src/ingestion.py
Multi-source ingestion: telemetry, fuel, maintenance datasets
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, DateType
)


# ── Schema Definitions ──────────────────────────────────────────────────────

TELEMETRY_SCHEMA = StructType([
    StructField("event_id",         StringType(),    False),
    StructField("vehicle_id",       StringType(),    False),
    StructField("driver_id",        StringType(),    True),
    StructField("event_timestamp",  TimestampType(), False),
    StructField("latitude",         DoubleType(),    True),
    StructField("longitude",        DoubleType(),    True),
    StructField("speed_kmh",        DoubleType(),    True),
    StructField("odometer_km",      DoubleType(),    True),
    StructField("engine_rpm",       IntegerType(),   True),
    StructField("harsh_braking",    IntegerType(),   True),
    StructField("harsh_accel",      IntegerType(),   True),
    StructField("idling_minutes",   DoubleType(),    True),
    StructField("distance_km",      DoubleType(),    True),
])

FUEL_SCHEMA = StructType([
    StructField("fuel_id",          StringType(),    False),
    StructField("vehicle_id",       StringType(),    False),
    StructField("fill_timestamp",   TimestampType(), False),
    StructField("litres_filled",    DoubleType(),    True),
    StructField("cost_zar",         DoubleType(),    True),
    StructField("odometer_km",      DoubleType(),    True),
    StructField("fuel_station_id",  StringType(),    True),
])

MAINTENANCE_SCHEMA = StructType([
    StructField("record_id",        StringType(),    False),
    StructField("vehicle_id",       StringType(),    False),
    StructField("service_date",     DateType(),      False),
    StructField("service_type",     StringType(),    True),
    StructField("next_service_km",  DoubleType(),    True),
    StructField("next_service_date",DateType(),      True),
    StructField("cost_zar",         DoubleType(),    True),
    StructField("workshop_id",      StringType(),    True),
    StructField("compliant",        IntegerType(),   True),   # 1 = on time, 0 = overdue
])


# ── Ingestion Functions ──────────────────────────────────────────────────────

def ingest_telemetry(spark: SparkSession, path: str):
    """
    Read raw GPS / telematics events from Parquet or CSV.
    Drops duplicate event IDs and nullifies impossible speed values.
    """
    df = (
        spark.read
        .option("header", "true")
        .schema(TELEMETRY_SCHEMA)
        .parquet(path)                          # swap to .csv() for flat files
        .dropDuplicates(["event_id"])
        .filter(F.col("speed_kmh").between(0, 200))
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("event_hour",  F.hour("event_timestamp"))
    )
    return df


def ingest_fuel(spark: SparkSession, path: str):
    """
    Read fuel-fill records.
    Rejects zero-litre fills and calculates cost per litre inline.
    """
    df = (
        spark.read
        .option("header", "true")
        .schema(FUEL_SCHEMA)
        .parquet(path)
        .dropDuplicates(["fuel_id"])
        .filter(F.col("litres_filled") > 0)
        .withColumn("fill_date",      F.to_date("fill_timestamp"))
        .withColumn("cost_per_litre", F.col("cost_zar") / F.col("litres_filled"))
    )
    return df


def ingest_maintenance(spark: SparkSession, path: str):
    """
    Read maintenance / service records.
    Derives overdue flag based on current date vs next_service_date.
    """
    today = F.current_date()
    df = (
        spark.read
        .option("header", "true")
        .schema(MAINTENANCE_SCHEMA)
        .parquet(path)
        .dropDuplicates(["record_id"])
        .withColumn(
            "is_overdue",
            F.when(F.col("next_service_date") < today, F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn(
            "days_until_service",
            F.datediff(F.col("next_service_date"), today)
        )
    )
    return df


def validate_join_keys(telemetry_df, fuel_df, maintenance_df):
    """
    Surface any vehicle IDs present in one source but missing in another.
    Returns a summary DataFrame for data-quality monitoring.
    """
    tele_ids  = telemetry_df.select("vehicle_id").distinct()
    fuel_ids  = fuel_df.select("vehicle_id").distinct()
    maint_ids = maintenance_df.select("vehicle_id").distinct()

    missing_fuel  = tele_ids.subtract(fuel_ids).withColumn("issue", F.lit("no_fuel_records"))
    missing_maint = tele_ids.subtract(maint_ids).withColumn("issue", F.lit("no_maintenance_records"))

    return missing_fuel.union(missing_maint)
