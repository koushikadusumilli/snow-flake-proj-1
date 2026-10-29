"""
### weather_flight_etl

Daily ELT pipeline: mock weather and flight data into Postgres, transformed
with dbt.

```
init_raw_schema --+--> load_raw_weather --+--> validate_raw_load
                  +--> load_raw_flights --+          |
                                                     v
                              dbt_build_staging --> dbt_build_marts
```

* **Extract and load** (`src/extract.py`): generates one UTC day of data and
  replaces that day's partition in `raw.weather_observations` and `raw.flights`.
* **Transform** (`dbt_project/`): `dbt build` runs models and their tests, so a
  failing data test stops the pipeline before the marts are refreshed.

Idempotent: each run covers exactly one data interval (the previous UTC day for
a scheduled or manually triggered run). Re-running or clearing any task is safe.
"""

from __future__ import annotations

import logging
import os
from datetime import date, timedelta
from typing import Any

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

from src import extract

logger = logging.getLogger(__name__)

# The container sets these (see docker-compose.yml). Defaults match that layout.
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt_project")
DBT_BUILD = (
    f"dbt build --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
)


def _to_day(timestamp: Any) -> date:
    """Convert Airflow's pendulum timestamp into a plain datetime.date."""
    return date(timestamp.year, timestamp.month, timestamp.day)


def _log_failure(context: dict[str, Any]) -> None:
    """Failure hook. Replace with Slack, email or PagerDuty in a real deployment."""
    task_instance = context["task_instance"]
    logger.error(
        "Task failed: dag=%s task=%s run_id=%s try=%s",
        task_instance.dag_id,
        task_instance.task_id,
        context.get("run_id"),
        task_instance.try_number,
    )


@dag(
    dag_id="weather_flight_etl",
    description="Load mock weather and flight data into Postgres and run dbt.",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,  # backfill explicitly with `airflow dags backfill`
    max_active_runs=1,  # keep runs sequential so backfills do not overlap
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": _log_failure,
    },
    tags=["portfolio", "elt", "dbt", "postgres"],
    doc_md=__doc__,
)
def weather_flight_etl() -> None:
    @task(task_id="init_raw_schema")
    def init_raw_schema() -> None:
        """Create the raw schema and tables (IF NOT EXISTS, safe every run)."""
        extract.init_raw_schema()

    @task(task_id="load_raw_weather")
    def load_raw_weather(data_interval_start=None) -> int:
        """Replace the weather partition for the run's data day."""
        return extract.load_weather(_to_day(data_interval_start))

    @task(task_id="load_raw_flights")
    def load_raw_flights(data_interval_start=None) -> int:
        """Replace the flights partition for the run's data day."""
        return extract.load_flights(_to_day(data_interval_start))

    @task(task_id="validate_raw_load")
    def validate_raw_load(
        weather_rows: int, flight_rows: int, data_interval_start=None
    ) -> None:
        """Confirm the committed row counts match what the loaders reported."""
        day = _to_day(data_interval_start)
        stored = extract.count_rows(day)
        expected = {"weather": weather_rows, "flights": flight_rows}
        if stored != expected or not all(stored.values()):
            raise ValueError(
                f"Raw load check failed for {day}: stored={stored} expected={expected}"
            )
        logger.info("Raw load check passed for %s: %s", day, stored)

    # dbt build = run models + run their tests, in dependency order.
    # Retries are limited because test failures are data problems, not blips.
    dbt_build_staging = BashOperator(
        task_id="dbt_build_staging",
        bash_command=f"{DBT_BUILD} --select path:models/staging",
        retries=1,
        execution_timeout=timedelta(minutes=15),
    )
    dbt_build_marts = BashOperator(
        task_id="dbt_build_marts",
        bash_command=f"{DBT_BUILD} --select path:models/marts",
        retries=1,
        execution_timeout=timedelta(minutes=15),
    )

    schema_ready = init_raw_schema()
    weather_rows = load_raw_weather()
    flight_rows = load_raw_flights()
    validated = validate_raw_load(weather_rows, flight_rows)

    schema_ready >> [weather_rows, flight_rows]
    validated >> dbt_build_staging >> dbt_build_marts


weather_flight_etl()
