# Kafka + Spark Streaming Pipeline

This project simulates a simple real-time data pipeline.

A Python producer generates user events and sends them to Kafka.  
Apache Spark reads those events, aggregates them by event type, and produces a dataset containing the results.

The entire pipeline runs with Docker Compose.

## Stack

- Python (event producer)
- Apache Kafka (message broker)
- Apache Spark (data processing)
- Docker Compose (orchestration)

## Architecture

Producer → Kafka → Spark → CSV

## Project Structure

```
kafka-spark-streaming-pipeline/
│
├── producer/
│   ├── Dockerfile
│   ├── producer.py
│   └── requirements.txt
│
├── spark/
│   ├── Dockerfile
│   └── spark_job.py
│
├── output/
│
├── docker-compose.yml
├── README.md
└── .gitignore
```


## Run the Project

Start the pipeline with:

    docker compose up --build

After the job finishes, the results will appear in:
    output/results/events_agg.csv

## Example Output

    event_type,count
    click,12
    view,14
    purchase,4


## What this project demonstrates

- Event streaming with Kafka
- Data processing with Spark
- Containerized data pipelines using Docker