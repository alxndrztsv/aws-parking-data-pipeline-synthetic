ARG AIRFLOW_VERSION="3.3.0"
ARG PYTHON_VERSION="3.13"
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# Switch to root user to install OS-level packages
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies as the airflow user
USER airflow
COPY requirements-airflow.txt .
RUN pip install --no-cache-dir -r requirements-airflow.txt