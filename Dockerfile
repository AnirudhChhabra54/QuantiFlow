FROM apache/airflow:2.8.1-python3.11

USER root
# Install system packages if needed
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements and install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Ensure /opt/airflow is on PYTHONPATH so `src` can be imported in DAGs
ENV PYTHONPATH=/opt/airflow
