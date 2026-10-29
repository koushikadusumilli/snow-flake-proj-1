# =============================================================================
# Airflow image with dbt installed in an isolated virtual environment.
#
# Why a separate venv for dbt?
#   Airflow pins hundreds of transitive dependencies. dbt-core pins its own set.
#   Installing both into one environment regularly produces resolver conflicts.
#   Keeping dbt in /home/airflow/dbt_venv avoids that, and the DAG simply calls
#   the `dbt` executable through BashOperator.
# =============================================================================
FROM apache/airflow:2.10.5-python3.10

COPY requirements.txt /tmp/requirements.txt

# PIP_USER=false: the base image may default pip to --user installs, which pip
# refuses to do inside a virtual environment.
RUN PIP_USER=false python -m venv /home/airflow/dbt_venv \
    && PIP_USER=false /home/airflow/dbt_venv/bin/pip install --no-cache-dir --upgrade pip \
    && PIP_USER=false /home/airflow/dbt_venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt

# Append (not prepend) so `python` and `pip` still resolve to Airflow's own
# environment, while `dbt` resolves to the venv.
ENV PATH="${PATH}:/home/airflow/dbt_venv/bin"
