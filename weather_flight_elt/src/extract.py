"""Extract mock weather and flight data and load it into the Postgres raw schema.

This module is the "EL" in ELT. It needs no API keys: it generates realistic
synthetic data, so the whole pipeline runs on the first try.

Design notes
------------
* Deterministic. Each day's data is generated from a random seed derived from
  the date, so re-running a day produces exactly the same rows.
* Idempotent. Every load replaces one date partition inside a single
  transaction (DELETE that day, INSERT that day). Re-running a day, or
  backfilling a range twice, never creates duplicates. DDL uses IF NOT EXISTS.
* Correlated. Flight delays and cancellations depend on the weather at the
  origin airport during the scheduled departure hour, so the final mart shows
  a real weather impact instead of noise.
* Slightly dirty on purpose. A small share of rows are duplicated, have
  missing values, mixed-case codes or inconsistent status labels. That gives
  the dbt staging layer real cleaning work to do.
* All timestamps are UTC.

Usage as a script (outside Airflow)
-----------------------------------
    python -m src.extract                       # load yesterday
    python -m src.extract --date 2026-10-01     # load one day
    python -m src.extract --date 2026-10-07 --days 7   # load 7 days ending then

Connection settings come from environment variables (defaults point at the
Docker warehouse published on localhost:5433):
    WAREHOUSE_HOST, WAREHOUSE_PORT, WAREHOUSE_DB, WAREHOUSE_USER,
    WAREHOUSE_PASSWORD
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import random
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, NamedTuple

import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

Row = dict[str, Any]

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------
# These identifiers are module-level constants, never user input, so building
# SQL strings from them is safe.
RAW_SCHEMA = "raw"
WEATHER_TABLE = "weather_observations"
FLIGHTS_TABLE = "flights"

FLIGHTS_PER_AIRPORT_PER_DAY = 60
DUPLICATE_WEATHER_RATE = 0.01  # share of weather rows emitted twice
MISSING_VISIBILITY_RATE = 0.02  # share of weather rows with no visibility
LOWERCASE_CODE_RATE = 0.03  # share of flight airport codes in lower case

WEATHER_COLUMNS = (
    "observation_id",
    "observation_date",
    "airport_code",
    "observed_at_utc",
    "temperature_f",
    "humidity_pct",
    "wind_speed_mph",
    "wind_gust_mph",
    "visibility_miles",
    "precipitation_in",
    "weather_condition",
)

FLIGHT_COLUMNS = (
    "flight_id",
    "flight_date",
    "carrier_code",
    "flight_number",
    "origin_airport",
    "destination_airport",
    "scheduled_departure_utc",
    "actual_departure_utc",
    "scheduled_arrival_utc",
    "actual_arrival_utc",
    "departure_delay_minutes",
    "arrival_delay_minutes",
    "flight_status",
    "aircraft_type",
    "distance_miles",
)

# -----------------------------------------------------------------------------
# DDL (idempotent)
# -----------------------------------------------------------------------------
DDL_STATEMENTS = (
    f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}",
    f"""
    CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{WEATHER_TABLE} (
        observation_id     TEXT        NOT NULL,
        observation_date   DATE        NOT NULL,
        airport_code       TEXT        NOT NULL,
        observed_at_utc    TIMESTAMPTZ NOT NULL,
        temperature_f      NUMERIC(5, 1),
        humidity_pct       INTEGER,
        wind_speed_mph     NUMERIC(5, 1),
        wind_gust_mph      NUMERIC(5, 1),
        visibility_miles   NUMERIC(5, 2),
        precipitation_in   NUMERIC(5, 2),
        weather_condition  TEXT,
        _loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    f"""
    CREATE INDEX IF NOT EXISTS idx_{WEATHER_TABLE}_observation_date
        ON {RAW_SCHEMA}.{WEATHER_TABLE} (observation_date)
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{FLIGHTS_TABLE} (
        flight_id                TEXT        NOT NULL,
        flight_date              DATE        NOT NULL,
        carrier_code             TEXT,
        flight_number            INTEGER,
        origin_airport           TEXT        NOT NULL,
        destination_airport      TEXT        NOT NULL,
        scheduled_departure_utc  TIMESTAMPTZ NOT NULL,
        actual_departure_utc     TIMESTAMPTZ,
        scheduled_arrival_utc    TIMESTAMPTZ,
        actual_arrival_utc       TIMESTAMPTZ,
        departure_delay_minutes  INTEGER,
        arrival_delay_minutes    INTEGER,
        flight_status            TEXT,
        aircraft_type            TEXT,
        distance_miles           INTEGER,
        _loaded_at               TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    f"""
    CREATE INDEX IF NOT EXISTS idx_{FLIGHTS_TABLE}_flight_date
        ON {RAW_SCHEMA}.{FLIGHTS_TABLE} (flight_date)
    """,
)


# -----------------------------------------------------------------------------
# Reference data
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class Airport:
    """Static reference data and a rough climate profile for one airport."""

    code: str
    latitude: float
    longitude: float
    mean_temp_f: float  # annual mean temperature
    seasonal_swing_f: float  # summer/winter amplitude around the mean
    precip_day_probability: float  # chance a given day is a precipitation day


AIRPORTS: tuple[Airport, ...] = (
    Airport("ATL", 33.64, -84.43, 62.0, 20.0, 0.30),
    Airport("ORD", 41.98, -87.90, 50.0, 26.0, 0.32),
    Airport("DFW", 32.90, -97.04, 66.0, 21.0, 0.22),
    Airport("DEN", 39.86, -104.67, 50.0, 22.0, 0.20),
    Airport("LAX", 33.94, -118.41, 65.0, 8.0, 0.07),
    Airport("JFK", 40.64, -73.78, 55.0, 22.0, 0.32),
    Airport("SEA", 47.45, -122.31, 53.0, 14.0, 0.40),
    Airport("MIA", 25.79, -80.29, 77.0, 7.0, 0.40),
    Airport("DTW", 42.21, -83.35, 50.0, 24.0, 0.35),
    Airport("MSP", 44.88, -93.22, 45.0, 29.0, 0.30),
)

CARRIERS = ("AA", "DL", "UA", "WN", "B6", "AS")
AIRCRAFT_TYPES = ("B737", "A320", "B738", "A321", "E175", "CRJ9", "B739")


class ConditionParams(NamedTuple):
    """Value ranges used to draw one hourly observation for a condition."""

    visibility: tuple[float, float]  # miles
    wind: tuple[float, float]  # mph
    precipitation: tuple[float, float]  # inches per hour (liquid equivalent)
    humidity: tuple[int, int]  # percent
    gust_probability: float


CONDITION_PARAMS: dict[str, ConditionParams] = {
    "Clear": ConditionParams((9.0, 10.0), (2, 12), (0.0, 0.0), (30, 70), 0.05),
    "Cloudy": ConditionParams((7.0, 10.0), (4, 15), (0.0, 0.0), (55, 85), 0.08),
    "Fog": ConditionParams((0.25, 2.0), (0, 6), (0.0, 0.0), (93, 100), 0.0),
    "Rain": ConditionParams((2.0, 6.0), (6, 18), (0.02, 0.30), (80, 98), 0.20),
    "Snow": ConditionParams((0.5, 4.0), (8, 22), (0.02, 0.25), (80, 98), 0.25),
    "Thunderstorm": ConditionParams((1.0, 5.0), (14, 30), (0.10, 0.80), (80, 98), 0.90),
}

# Base probability contribution of each condition to a flight disruption.
WEATHER_DISRUPTION_RISK: dict[str, float] = {
    "Clear": 0.00,
    "Cloudy": 0.02,
    "Fog": 0.25,
    "Rain": 0.12,
    "Snow": 0.30,
    "Thunderstorm": 0.40,
}

# Raw status labels are intentionally inconsistent. dbt normalises them.
STATUS_LABELS: dict[str, tuple[str, ...]] = {
    "on_time": ("On Time", "On Time", "On Time", "ON TIME", "on-time"),
    "delayed": ("Delayed", "Delayed", "DELAYED"),
    "cancelled": ("Cancelled", "Cancelled", "CANCELED"),
}


# -----------------------------------------------------------------------------
# Weather generation
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class DayPlan:
    """The broad shape of one airport's weather for one day."""

    mean_temp_f: float
    regime: str  # dominant condition for the day


def _plan_day(rng: random.Random, airport: Airport, day: date) -> DayPlan:
    """Pick a daily mean temperature and a dominant weather regime."""
    day_of_year = day.timetuple().tm_yday
    # -cos peaks in mid-July and bottoms out in mid-January (northern hemisphere).
    seasonal = -math.cos(2 * math.pi * (day_of_year - 15) / 365)
    mean_temp = airport.mean_temp_f + airport.seasonal_swing_f * seasonal
    mean_temp += rng.gauss(0, 5)

    if rng.random() < airport.precip_day_probability:
        if mean_temp <= 34:
            regime = "Snow"
        elif mean_temp >= 60 and rng.random() < 0.20:
            regime = "Thunderstorm"
        else:
            regime = "Rain"
    elif rng.random() < 0.06:
        regime = "Fog"
    else:
        regime = rng.choice(("Clear", "Clear", "Cloudy"))
    return DayPlan(mean_temp_f=mean_temp, regime=regime)


def _hourly_condition(rng: random.Random, regime: str, hour: int) -> str:
    """Turn the daily regime into the condition observed in a given hour."""
    if regime in ("Rain", "Snow", "Thunderstorm"):
        # Precipitation comes and goes through the day.
        return regime if rng.random() < 0.65 else "Cloudy"
    if regime == "Fog":
        # Fog burns off by mid-day (UTC hour is only a rough proxy here).
        return "Fog" if hour <= 15 and rng.random() < 0.8 else "Clear"
    return regime


def _observe(
    rng: random.Random, airport: Airport, day: date, hour: int, plan: DayPlan
) -> Row:
    """Build one hourly weather observation."""
    condition = _hourly_condition(rng, plan.regime, hour)
    params = CONDITION_PARAMS[condition]

    # Warmest around 20:00 UTC, coolest around 08:00 UTC.
    diurnal = 7.0 * math.cos(2 * math.pi * (hour - 20) / 24)
    temperature = plan.mean_temp_f + diurnal + rng.gauss(0, 1.2)

    wind = rng.uniform(*params.wind)
    gust = None
    if rng.random() < params.gust_probability:
        gust = wind + rng.uniform(8, 25)

    visibility = rng.uniform(*params.visibility)
    if rng.random() < MISSING_VISIBILITY_RATE:
        visibility = None

    observed_at = datetime(day.year, day.month, day.day, hour, 51, tzinfo=timezone.utc)
    return {
        "observation_id": f"{airport.code}-{observed_at:%Y%m%dT%H%M}",
        "observation_date": day,
        "airport_code": airport.code,
        "observed_at_utc": observed_at,
        "temperature_f": round(temperature, 1),
        "humidity_pct": rng.randint(*params.humidity),
        "wind_speed_mph": round(wind, 1),
        "wind_gust_mph": None if gust is None else round(gust, 1),
        "visibility_miles": None if visibility is None else round(visibility, 2),
        "precipitation_in": round(rng.uniform(*params.precipitation), 2),
        "weather_condition": condition,
    }


def generate_weather_rows(day: date) -> list[Row]:
    """Generate 24 hourly observations per airport for one UTC day.

    A small share of rows is emitted twice to mimic an at-least-once source.
    """
    rng = random.Random(f"weather|{day.isoformat()}")
    rows: list[Row] = []
    for airport in AIRPORTS:
        plan = _plan_day(rng, airport, day)
        for hour in range(24):
            rows.append(_observe(rng, airport, day, hour, plan))

    duplicate_count = max(1, int(len(rows) * DUPLICATE_WEATHER_RATE))
    rows.extend(dict(row) for row in rng.sample(rows, k=duplicate_count))
    return rows


# -----------------------------------------------------------------------------
# Flight generation
# -----------------------------------------------------------------------------
def _distance_miles(a: Airport, b: Airport) -> int:
    """Great-circle distance between two airports (haversine), in miles."""
    earth_radius_miles = 3958.8
    lat1, lon1, lat2, lon2 = map(
        math.radians, (a.latitude, a.longitude, b.latitude, b.longitude)
    )
    h = (
        math.sin((lat2 - lat1) / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    )
    return int(round(2 * earth_radius_miles * math.asin(math.sqrt(h))))


def _disruption_risk(weather: Row | None) -> float:
    """Convert an hourly observation into a 0..0.85 disruption risk score."""
    if weather is None:
        return 0.0
    risk = WEATHER_DISRUPTION_RISK[weather["weather_condition"]]
    gust = weather["wind_gust_mph"] or weather["wind_speed_mph"]
    if gust >= 30:
        risk += 0.10
    visibility = weather["visibility_miles"]
    if visibility is not None and visibility < 1.0:
        risk += 0.15
    return min(risk, 0.85)


def generate_flight_rows(day: date, weather_rows: list[Row]) -> list[Row]:
    """Generate departures for every airport on one UTC day.

    ``weather_rows`` is the output of :func:`generate_weather_rows` for the same
    day. It drives the delay and cancellation probabilities.
    """
    rng = random.Random(f"flights|{day.isoformat()}")
    weather_lookup = {
        (w["airport_code"], w["observed_at_utc"].hour): w for w in weather_rows
    }

    rows: list[Row] = []
    for origin in AIRPORTS:
        destinations = [a for a in AIRPORTS if a.code != origin.code]
        for seq in range(FLIGHTS_PER_AIRPORT_PER_DAY):
            destination = rng.choice(destinations)
            carrier = rng.choice(CARRIERS)
            flight_number = rng.randint(100, 2999)

            # Departures cluster in 10:00-23:59 UTC, a rough US daytime proxy.
            hour = rng.randint(10, 23)
            minute = rng.randrange(0, 60, 5)
            scheduled_dep = datetime(
                day.year, day.month, day.day, hour, minute, tzinfo=timezone.utc
            )
            distance = _distance_miles(origin, destination)
            block_minutes = int(distance / 8.0 + 30 + rng.randint(-5, 10))
            scheduled_arr = scheduled_dep + timedelta(minutes=block_minutes)

            risk = _disruption_risk(weather_lookup.get((origin.code, hour)))
            cancel_probability = 0.004 + 0.10 * risk
            delay_probability = min(0.10 + 0.60 * risk, 0.90)

            roll = rng.random()
            if roll < cancel_probability:
                status = "cancelled"
                dep_delay = arr_delay = None
            else:
                if roll < cancel_probability + delay_probability:
                    status = "delayed"
                    mean_delay = 25 + 80 * risk
                    dep_delay = 15 + int(rng.expovariate(1 / mean_delay))
                else:
                    status = "on_time"
                    dep_delay = rng.randint(-8, 14)  # under the 15 minute threshold
                arr_delay = dep_delay + rng.randint(-12, 8)  # some time made up en route

            origin_code = origin.code
            if rng.random() < LOWERCASE_CODE_RATE:
                origin_code = origin_code.lower()
            destination_code = destination.code
            if rng.random() < LOWERCASE_CODE_RATE:
                destination_code = destination_code.lower()

            rows.append(
                {
                    "flight_id": f"{day:%Y%m%d}-{origin.code}-{seq:03d}-{carrier}{flight_number}",
                    "flight_date": day,
                    "carrier_code": carrier,
                    "flight_number": flight_number,
                    "origin_airport": origin_code,
                    "destination_airport": destination_code,
                    "scheduled_departure_utc": scheduled_dep,
                    "actual_departure_utc": (
                        None
                        if dep_delay is None
                        else scheduled_dep + timedelta(minutes=dep_delay)
                    ),
                    "scheduled_arrival_utc": scheduled_arr,
                    "actual_arrival_utc": (
                        None
                        if arr_delay is None
                        else scheduled_arr + timedelta(minutes=arr_delay)
                    ),
                    "departure_delay_minutes": dep_delay,
                    "arrival_delay_minutes": arr_delay,
                    "flight_status": rng.choice(STATUS_LABELS[status]),
                    "aircraft_type": rng.choice(AIRCRAFT_TYPES),
                    "distance_miles": distance,
                }
            )
    return rows


# -----------------------------------------------------------------------------
# Database helpers
# -----------------------------------------------------------------------------
def get_connection():
    """Open a connection to the warehouse using WAREHOUSE_* environment variables."""
    return psycopg2.connect(
        host=os.getenv("WAREHOUSE_HOST", "localhost"),
        port=int(os.getenv("WAREHOUSE_PORT", "5433")),
        dbname=os.getenv("WAREHOUSE_DB", "warehouse"),
        user=os.getenv("WAREHOUSE_USER", "warehouse"),
        password=os.getenv("WAREHOUSE_PASSWORD", "warehouse"),
        connect_timeout=10,
    )


def init_raw_schema() -> None:
    """Create the raw schema, tables and indexes if they do not exist."""
    with closing(get_connection()) as conn, conn, conn.cursor() as cur:
        for statement in DDL_STATEMENTS:
            cur.execute(statement)
    logger.info("Raw schema is ready")


def _replace_partition(
    table: str, date_column: str, day: date, columns: tuple[str, ...], rows: list[Row]
) -> int:
    """Replace one date partition of a raw table in a single transaction.

    DELETE plus INSERT inside one transaction means readers never see a
    half-loaded day, and running the same day twice leaves the same data.
    """
    values = [tuple(row[column] for column in columns) for row in rows]
    delete_sql = f"DELETE FROM {RAW_SCHEMA}.{table} WHERE {date_column} = %s"
    insert_sql = f"INSERT INTO {RAW_SCHEMA}.{table} ({', '.join(columns)}) VALUES %s"

    with closing(get_connection()) as conn, conn, conn.cursor() as cur:
        cur.execute(delete_sql, (day,))
        execute_values(cur, insert_sql, values, page_size=1000)
    return len(values)


def load_weather(day: date) -> int:
    """Generate and load one day of weather observations. Returns rows loaded."""
    rows = generate_weather_rows(day)
    count = _replace_partition(
        WEATHER_TABLE, "observation_date", day, WEATHER_COLUMNS, rows
    )
    logger.info("Loaded %d weather rows for %s", count, day)
    return count


def load_flights(day: date) -> int:
    """Generate and load one day of flights. Returns rows loaded.

    Weather is regenerated here (it is deterministic and cheap) so that this
    task does not depend on the weather task having run first.
    """
    rows = generate_flight_rows(day, generate_weather_rows(day))
    count = _replace_partition(FLIGHTS_TABLE, "flight_date", day, FLIGHT_COLUMNS, rows)
    logger.info("Loaded %d flight rows for %s", count, day)
    return count


def count_rows(day: date) -> dict[str, int]:
    """Count raw rows stored for one day. Used for post-load validation."""
    queries = {
        "weather": f"SELECT count(*) FROM {RAW_SCHEMA}.{WEATHER_TABLE} "
        "WHERE observation_date = %s",
        "flights": f"SELECT count(*) FROM {RAW_SCHEMA}.{FLIGHTS_TABLE} "
        "WHERE flight_date = %s",
    }
    counts: dict[str, int] = {}
    with closing(get_connection()) as conn, conn, conn.cursor() as cur:
        for name, query in queries.items():
            cur.execute(query, (day,))
            counts[name] = cur.fetchone()[0]
    return counts


def run_extract_load(day: date) -> dict[str, int]:
    """Create tables if needed, then load weather and flights for one day."""
    init_raw_schema()
    return {"weather": load_weather(day), "flights": load_flights(day)}


# -----------------------------------------------------------------------------
# Command line entry point
# -----------------------------------------------------------------------------
def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    parser = argparse.ArgumentParser(
        description="Load mock weather and flight data into the raw schema."
    )
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=yesterday,
        help="Last day to load, YYYY-MM-DD (default: yesterday, UTC)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=1,
        help="Number of days to load, ending on --date (default: 1)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    args = _parse_args(argv)
    for offset in range(args.days - 1, -1, -1):
        day = args.date - timedelta(days=offset)
        counts = run_extract_load(day)
        logger.info("Day %s done: %s", day, counts)


if __name__ == "__main__":
    main()
