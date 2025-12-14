# 🏆 ELT Pipeline for PUBG Player Match Stats

An end-to-end ELT data pipeline that transforms raw gameplay telemetry into actionable esports insights.

This project automates the extraction of high-volume match data from the PUBG Official API, loads it into Google BigQuery, and transforms it using dbt to calculate advanced metrics.

Built with Python, Docker, Airflow, and SQL, it is designed to handle complex, semi-structured JSON events at scale, turning thousands of combat logs into a clean, queryable Star Schema for visualization.

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
    git clone https://github.com/HarshKumarGuptagit/pubg-analytics-end-to-end-pipeline.git
    # Add your 'gcp_key.json' to /keys and set PUBG_API_KEY in .env
    ```

2.  **Run Pipeline**
    ```bash
    docker-compose up -d
    docker-compose run --rm airflow-webserver airflow dags trigger pubg_analytics_e2e
    ```
## ☁️ Cloud Deployment Architecture : **Google Cloud Platform (GCP)**. 

Below is the production mapping from the local Docker environment to managed cloud services:

| Local Component (Docker) | Production Service (GCP) | Purpose |
| :--- | :--- | :--- |
| **Orchestrator** (Airflow Container) | **Cloud Composer** | Fully managed Airflow environment for scheduling DAGs and monitoring pipeline health. |
| **Ingestion Script** (Python Container) | **Cloud Run Jobs** | Serverless execution for the data extraction script. Allows the job to scale to 0 when not running (cost-efficient). |
| **Transformation** (dbt Container) | **Cloud Build / Cloud Run** | dbt runs are triggered as containerized tasks immediately after ingestion completes. |
| **Data Warehouse** (BigQuery Local) | **BigQuery** | Serverless data warehouse. Autoscales storage and compute based on query complexity. |
| **Secrets** (.env file) | **Secret Manager** | Securely encrypts and stores the `PUBG_API_KEY` and service account credentials. |
| **Storage** (Local Disk) | **Google Cloud Storage (GCS)** | Acts as the **Data Lake** for raw JSON backups and stores Airflow logs. |



    
