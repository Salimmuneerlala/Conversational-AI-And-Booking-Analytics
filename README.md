# Conversational-AI-And-Booking-Analytics Pipeline – Azure Data Engineering Project

This project implements a **scalable end-to-end data engineering pipeline** for processing chatbot interaction and booking data using Azure services and Databricks.

It is designed to transform raw conversational data into **actionable business insights** such as funnel analysis, conversion tracking, and user behavior analytics.

---

## Problem Statement

Organizations using chatbot systems often lack visibility into:

* How users interact with the chatbot
* Where users drop in the conversation flow
* Which interactions lead to bookings
* Performance across cities, dealers, and products

This project addresses these challenges by building a **data pipeline that connects user behavior with business outcomes**.

---

## Architecture Overview

The pipeline follows the **Medallion Architecture (Bronze → Silver → Gold)** for structured and scalable data processing.

![Architecture Diagram](docs/images/ArchitectureDiagram.png)

---

## Tech Stack

* **Azure Data Factory (ADF)** – Data ingestion & orchestration
* **Azure Data Lake Storage Gen2 (ADLS)** – Data storage
* **Azure Databricks (PySpark)** – Distributed data processing
* **Delta Lake** – ACID transactions & data reliability
* **Power BI** – Data visualization (optional)

---

## Data Pipeline Flow

```text
Azure SQL DB → ADF (Incremental Load) → ADLS Bronze → Databricks (Silver) → Databricks (Gold) → BI / Dashboard
```

---

## Bronze Layer – Raw Data

* Data ingested from **Azure SQL Database** using ADF
* Incremental load using **watermark logic**
* Stored in **ADLS Gen2 (Parquet format)**
* No transformations (source of truth)

---

## Silver Layer – Transformation

Handled in **Azure Databricks using PySpark**

Key transformations:

* Data cleaning (null handling, type casting)
* Sessionization using window functions
* Step ordering for user journey tracking
* Time-based join between sessions and bookings
* Data quality checks + quarantine handling
* Creation of session-level dataset

---

## Data Modeling

* Fact-like dataset representing user sessions
* Dimension tables (Dealer, Product)
* **SCD Type 2** implementation using Delta Lake MERGE
* Surrogate keys using identity columns

---

## Gold Layer – Business Insights

* Funnel analysis (engagement → conversion)
* Drop-off analysis
* City-wise and product-wise performance
* Aggregated datasets for reporting

---

## Incremental Processing

* Watermark-based ingestion ensures:

  * Only new data is processed
  * Efficient pipeline execution
  * Idempotent behavior

---

## Data Quality & Reliability

* Null validation for critical fields
* Quarantine handling for invalid records
* Schema drift support (ADF + Databricks)
* Delta Lake ensures:

  * ACID transactions
  * Versioning
  * Reliable updates (MERGE)

---

## Performance Optimization

* Partitioning by date for faster queries
* Efficient Parquet/Delta storage
* Distributed processing using PySpark
* Reduced data scan and improved query performance

---

## Project Structure

```text
.
├── adf/                # ADF pipelines & screenshots
├── docs/               # Architecture & documentation
│   └── images/
├── databricks/         # Notebooks (Silver & Gold logic)
└── README.md
```

---

## Key Outcomes

* Built scalable ETL pipeline processing large volumes of chatbot data
* Enabled accurate session-based analytics using distributed processing
* Improved conversion tracking and funnel visibility
* Delivered business insights for decision-making

---

## Summary

This project demonstrates:

* End-to-end data pipeline design
* Strong understanding of Medallion Architecture
* Advanced PySpark transformations (sessionization, joins)
* Production-ready practices (incremental load, SCD, Delta Lake)

---

## Future Enhancements

* Real-time ingestion using streaming (Kafka/Event Hub)
* Advanced data quality frameworks (e.g., Great Expectations)
* Automated monitoring and alerting
* Dashboard implementation in Power BI

---
