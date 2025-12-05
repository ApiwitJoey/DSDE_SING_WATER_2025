FROM apache/airflow:2.10.3-python3.12

USER root

# 1. ลง Chromium และ Chromium Driver (ง่ายกว่าและเสถียรกว่า)
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver \
    build-essential \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt