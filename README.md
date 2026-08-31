# 🎧 Spotify Real-Time & Batch Data Pipeline (End-to-End MDS)

An enterprise-grade, end-to-end Data Engineering pipeline that simulates real-time Spotify user listening events, ingests data into object storage, orchestrates batch ingestion to Snowflake using Apache Airflow, and visualizes KPIs in Power BI.

---

## 🏗️ Architecture Overview

1. **Producer:** Python simulator generating real-time user activity events (Song play, playlist additions, device types, geo-locations).
2. **Streaming Layer:** Apache Kafka for distributed event streaming.
3. **Landing Layer (Bronze):** Custom Kafka Python Consumer ingesting batched raw JSON events into MinIO (S3-compatible Object Storage).
4. **Orchestration:** Apache Airflow DAG extracting staged batches and performing bulk batch loading.
5. **Data Warehouse:** Snowflake (Medallion Architecture - Bronze RAW layer).
6. **Analytics & BI:** Interactive Power BI Dashboard tracking streaming trends, top artists, device analytics, and geographical heatmaps.

---

## 🛠️ Tech Stack

* **Streaming:** Apache Kafka, Confluent-Kafka
* **Object Storage:** MinIO (Local S3)
* **Orchestration:** Apache Airflow (Dockerized)
* **Data Warehouse:** Snowflake
* **BI & Visuals:** Microsoft Power BI
* **Containerization:** Docker & Docker Compose
* **Programming:** Python 3.12, SQL

---

## ⚙️ Setup & Installation

### 1. Clone the repository
```bash
git clone [https://github.com/](https://github.com/)MohammadQaiyyumAnsari/spotify-realtime-data-pipeline.git
cd spotify-realtime-data-pipeline
