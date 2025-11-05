# Use official lightweight Airflow image (Python 3.10)
FROM apache/airflow:2.9.3

# Switch to root to install system packages safely
USER root

# Install lightweight dependencies and cleanup immediately
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc g++ curl git && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy your Python dependencies
COPY requirements.txt /requirements.txt

# Switch to airflow user (REQUIRED by official image)
USER airflow

# Install Python packages safely inside Airflow environment
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt && \
    pip cache purge

# Copy your Airflow DAGs and project files
COPY ./valorant_scraper /opt/airflow/

# Environment configuration
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
ENV AIRFLOW_HOME=/opt/airflow