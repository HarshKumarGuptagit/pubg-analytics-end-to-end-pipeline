# 🏆 ELT Pipeline for PUBG Player Match Stats

A robust data pipeline that extracts granular esports match data from the PUBG API, loads it directly into BigQuery, and transforms it into actionable insights using dbt.

## 🏗️ Architecture

![PUBG ELT Pipeline Architecture](assets/PUBG%20ELT%20Pipeline.png)

## 🛠️ Tech Stack

* **Ingestion:** Python (Dockerized)
* **Orchestration:** Apache Airflow
* **Warehouse:** Google BigQuery (JSON-native storage)
* **Transformation:** dbt (Data Build Tool)
* **Visualization:** Looker Studio

## 🚀 Quick Start

1.  **Clone & Configure**
    ```bash
    git clone [https://github.com/your-username/pubg-analytics-pipeline.git](https://github.com/your-username/pubg-analytics-pipeline.git)
    # Add your 'gcp_key.json' to /keys and set PUBG_API_KEY in .env
    ```

2.  **Run Pipeline**
    ```bash
    docker-compose up -d
    docker-compose run --rm airflow-webserver airflow dags trigger pubg_analytics_e2e
    ```