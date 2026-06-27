"""
fleet-operations-analytics/data/generate_sample_data.py
Generates synthetic telemetry, fuel, and maintenance datasets as Parquet files.
Run once to populate the data/ directory for local development and testing.

Usage:
    spark-submit data/generate_sample_data.py
    or call generate_all() from a Databricks notebook cell.
"""

import random
from datetime import date, datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, DateType
)


# ── Config ───────────────────────────────────────────────────────────────────

NUM_VEHICLES        = 20
NUM_DRIVERS         = 15
OBSERVATION_DAYS    = 30
START_DATE          = date(2024, 5, 1)
EVENTS_PER_DAY_RANGE = (1, 5)      # telemetry events per vehicle per active day
FUEL_FILL_INTERVAL_DAYS = 5        # rough fill frequency per vehicle
SEED                = 42

OUTPUT_DIR = "data/sample"         # relative; update to ADLS path for cloud runs


# ── Helpers ──────────────────────────────────────────────────────────────────

def date_range(start: date, days: int):
    return [start + timedelta(days=i) for i in range(days)]


def rand_id(prefix: str, n: int) -> list:
    return [f"{prefix}{str(i).zfill(3)}" for i in range(1, n + 1)]


# ── Telemetry Generator ───────────────────────────────────────────────────────

def generate_telemetry(spark: SparkSession) -> None:
    random.seed(SEED)

    vehicle_ids = rand_id("V", NUM_VEHICLES)
    driver_ids  = rand_id("D", NUM_DRIVERS)
    dates       = date_range(START_DATE, OBSERVATION_DAYS)

    rows = []
    event_counter = 1

    for vehicle_id in vehicle_ids:
        odometer = random.uniform(20_000, 150_000)

        # Assign a primary driver; occasionally swap
        primary_driver = random.choice(driver_ids)

        for day in dates:
            # ~80% chance a vehicle is active on any given day
            if random.random() > 0.80:
                continue

            n_events = random.randint(*EVENTS_PER_DAY_RANGE)

            for _ in range(n_events):
                hour    = random.randint(5, 21)
                minute  = random.randint(0, 59)
                ts      = datetime(day.year, day.month, day.day, hour, minute)

                distance   = round(random.uniform(5, 80), 2)
                odometer  += distance

                # Inject poor performers for realism
                is_bad_driver = vehicle_id in ["V003", "V007", "V011"]
                harsh_brake   = random.randint(3, 8) if is_bad_driver else random.randint(0, 2)
                harsh_accel   = random.randint(2, 6) if is_bad_driver else random.randint(0, 1)
                speed         = round(random.uniform(130, 160), 1) if is_bad_driver else round(random.uniform(40, 115), 1)
                idling        = round(random.uniform(20, 60), 1)   if is_bad_driver else round(random.uniform(0, 20), 1)

                driver = primary_driver if random.random() > 0.1 else random.choice(driver_ids)
                lat    = round(random.uniform(-26.5, -25.5), 6)    # Gauteng bounding box
                lon    = round(random.uniform(27.5, 28.5), 6)

                rows.append((
                    f"E{str(event_counter).zfill(6)}",
                    vehicle_id,
                    driver,
                    ts,
                    lat,
                    lon,
                    speed,
                    round(odometer, 2),
                    random.randint(800, 3500),
                    harsh_brake,
                    harsh_accel,
                    idling,
                    distance,
                ))
                event_counter += 1

    schema = StructType([
        StructField("event_id",        StringType(),    False),
        StructField("vehicle_id",      StringType(),    False),
        StructField("driver_id",       StringType(),    True),
        StructField("event_timestamp", TimestampType(), False),
        StructField("latitude",        DoubleType(),    True),
        StructField("longitude",       DoubleType(),    True),
        StructField("speed_kmh",       DoubleType(),    True),
        StructField("odometer_km",     DoubleType(),    True),
        StructField("engine_rpm",      IntegerType(),   True),
        StructField("harsh_braking",   IntegerType(),   True),
        StructField("harsh_accel",     IntegerType(),   True),
        StructField("idling_minutes",  DoubleType(),    True),
        StructField("distance_km",     DoubleType(),    True),
    ])

    df = spark.createDataFrame(rows, schema=schema)

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .parquet(f"{OUTPUT_DIR}/telemetry")
    )
    print(f"Telemetry: {df.count()} events written to {OUTPUT_DIR}/telemetry")


# ── Fuel Generator ────────────────────────────────────────────────────────────

def generate_fuel(spark: SparkSession) -> None:
    random.seed(SEED + 1)

    vehicle_ids   = rand_id("V", NUM_VEHICLES)
    station_ids   = rand_id("STN", 8)
    dates         = date_range(START_DATE, OBSERVATION_DAYS)
    fuel_counter  = 1
    rows          = []

    for vehicle_id in vehicle_ids:
        odometer = random.uniform(20_000, 150_000)
        fill_days = [d for d in dates if random.random() < (1 / FUEL_FILL_INTERVAL_DAYS)]

        # Inject poor fuel efficiency for specific vehicles
        is_inefficient = vehicle_id in ["V003", "V007", "V015", "V018"]

        for day in fill_days:
            litres = round(random.uniform(40, 70) if is_inefficient else random.uniform(18, 45), 2)
            cost   = round(litres * random.uniform(22.5, 24.5), 2)    # ZAR per litre
            odometer += random.uniform(150, 350)

            ts = datetime(day.year, day.month, day.day,
                          random.randint(6, 20), random.randint(0, 59))

            rows.append((
                f"F{str(fuel_counter).zfill(5)}",
                vehicle_id,
                ts,
                litres,
                cost,
                round(odometer, 2),
                random.choice(station_ids),
            ))
            fuel_counter += 1

    schema = StructType([
        StructField("fuel_id",         StringType(),    False),
        StructField("vehicle_id",      StringType(),    False),
        StructField("fill_timestamp",  TimestampType(), False),
        StructField("litres_filled",   DoubleType(),    True),
        StructField("cost_zar",        DoubleType(),    True),
        StructField("odometer_km",     DoubleType(),    True),
        StructField("fuel_station_id", StringType(),    True),
    ])

    df = spark.createDataFrame(rows, schema=schema)

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .parquet(f"{OUTPUT_DIR}/fuel")
    )
    print(f"Fuel: {df.count()} records written to {OUTPUT_DIR}/fuel")


# ── Maintenance Generator ─────────────────────────────────────────────────────

def generate_maintenance(spark: SparkSession) -> None:
    random.seed(SEED + 2)

    vehicle_ids      = rand_id("V", NUM_VEHICLES)
    workshop_ids     = rand_id("WS", 5)
    service_types    = ["Oil Change", "Major Service", "Brake Inspection",
                        "Tyre Rotation", "Filter Replacement", "Full Service"]
    maint_counter    = 1
    rows             = []

    for vehicle_id in vehicle_ids:
        # Each vehicle has 1–3 historical service records
        n_records = random.randint(1, 3)

        # Inject non-compliant vehicles
        is_noncompliant = vehicle_id in ["V005", "V009", "V011", "V017"]

        service_date = START_DATE - timedelta(days=random.randint(10, 120))

        for i in range(n_records):
            compliant      = 0 if is_noncompliant and random.random() < 0.7 else 1
            cost           = round(random.uniform(500, 6000), 2)
            next_km        = round(random.uniform(5000, 15000), 0)
            next_svc_delta = random.randint(-15, 90)      # negative = already overdue
            next_svc_date  = date.today() + timedelta(days=next_svc_delta)

            rows.append((
                f"M{str(maint_counter).zfill(5)}",
                vehicle_id,
                service_date,
                random.choice(service_types),
                next_km,
                next_svc_date,
                cost,
                random.choice(workshop_ids),
                compliant,
            ))
            maint_counter  += 1
            service_date   += timedelta(days=random.randint(30, 90))

    schema = StructType([
        StructField("record_id",         StringType(),  False),
        StructField("vehicle_id",        StringType(),  False),
        StructField("service_date",      DateType(),    False),
        StructField("service_type",      StringType(),  True),
        StructField("next_service_km",   DoubleType(),  True),
        StructField("next_service_date", DateType(),    True),
        StructField("cost_zar",          DoubleType(),  True),
        StructField("workshop_id",       StringType(),  True),
        StructField("compliant",         IntegerType(), True),
    ])

    df = spark.createDataFrame(rows, schema=schema)

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .parquet(f"{OUTPUT_DIR}/maintenance")
    )
    print(f"Maintenance: {df.count()} records written to {OUTPUT_DIR}/maintenance")


# ── Entry Point ───────────────────────────────────────────────────────────────

def generate_all():
    spark = (
        SparkSession.builder
        .appName("FleetSampleDataGenerator")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    print(f"Generating {NUM_VEHICLES} vehicles over {OBSERVATION_DAYS} days...")
    generate_telemetry(spark)
    generate_fuel(spark)
    generate_maintenance(spark)
    print("Sample data generation complete.")
    spark.stop()


if __name__ == "__main__":
    generate_all()
