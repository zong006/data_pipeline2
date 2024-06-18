# Sales Data Streaming and ETL Pipeline

This project implements a real-time data pipeline for streaming sales data from a producer to Kafka, processing it with Spark Streaming, and persisting it to PostgreSQL. This project builds on the streaming pipeline set-up in [this GitHub repository](https://github.com/zong006/data_pipeline), by adding a component for scheduled batch processing using Airflow that aggregates and processes the sales data, finally persisting it to PostgreSQL.


## Components

### 1. Kafka Cluster
Kafka acts as the message broker for the data pipeline. The Kafka cluster is hosted on Upstash, providing a scalable and reliable platform for handling streaming data.

### 2. Kafka Producer
The Kafka Producer component generates simulated sales data and publishes it as messages to a Kafka topic. The producer script is `producer.py`, located in the `streaming/` folder.

### 3. Spark Streaming Consumer
Spark Streaming consumes messages from the Kafka topic and processes them in real-time. Spark writes the processed data to a PostgreSQL table named `order_items`. The consumer script is `spark-postgres.py`, located in the `streaming/` folder.

### 4. PostgreSQL Database
PostgreSQL is used to persist the streaming and processed data. In this project, PostgreSQL is run as a Docker container. The initial sales data is streamed and stored in the `order_items` table. The processed data after batch processing is used to update the `hourly_sales_data` table (for a simulation purpose, the sales data is aggregated by the hour). 

#### Tables
The `order_items` table stores the raw streaming sales data.

| Column           | Type    | Description           |
|------------------|---------|-----------------------|
| `product_id`     | INTEGER | Product identifier    |
| `quantity`       | INTEGER | Quantity sold         |
| `price_per_unit` | FLOAT   | Price per unit        |
| `timestamp`      | STRING  | Timestamp of the sale |

The `hourly_sales_data` table stores the aggregated hourly sales data by item category.

| Column         | Type    | Description                      |
|----------------|---------|----------------------------------|
| `hour_of_day`  | INTEGER | Hour of the day (0-23)           |
| `Kitchen`      | FLOAT   | Total sales in Kitchen category  |
| `Sports`       | FLOAT   | Total sales in Sports category   |
| `Tools`        | FLOAT   | Total sales in Tools category    |
| `Fashion`      | FLOAT   | Total sales in Fashion category  |
| `Widgets`      | FLOAT   | Total sales in Widgets category  |
| `Electronics`  | FLOAT   | Total sales in Electronics category |
| `Gadgets`      | FLOAT   | Total sales in Gadgets category  |
| `Home`         | FLOAT   | Total sales in Home category     |
| `Office`       | FLOAT   | Total sales in Office category   |

### 5. Airflow for ETL Batch Processing
Airflow is used to schedule and manage the ETL process. The ETL pipeline performs a join with the table from `dags/data/products.csv`, then aggregates and processes the hourly revenue of sales by item category, updating the results in a PostgreSQL table named `hourly_sales_data`. The Airflow DAG script is `batch_process.py`, located in the `dags/` folder.



