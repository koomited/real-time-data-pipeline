# Real Time Data Pipeline
[![Linux](https://img.shields.io/badge/Linux-FCC624?logo=linux&logoColor=black)](#)
[![Visual Studio Code](https://img.shields.io/badge/Visual%20Studio%20Code-0078d7.svg?logo=visual-studio-code&logoColor=white)](#)
[![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)](#)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?logo=postgresql&logoColor=white)](#)
[![GitHub](https://img.shields.io/badge/GitHub-%23121011.svg?logo=github&logoColor=white)](#)
[![Git](https://img.shields.io/badge/Git-F05032?logo=git&logoColor=white)](#)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?logo=apachekafka&logoColor=white)](#)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?logo=apachespark&logoColor=white)](#)
[![MongoDB](https://img.shields.io/badge/MongoDB-47A248?logo=mongodb&logoColor=white)](#)
[![Elasticsearch](https://img.shields.io/badge/Elasticsearch-005571?logo=elasticsearch&logoColor=white)](#)
![Awesome](https://img.shields.io/badge/Awesome-ffd700?logo=awesome&logoColor=black)
---
This project is in the framework of my data engineering zoomcamp. It combines streaming data processing using kafka, spark, mongodb, elastic search



A real-time  data pipeline project using Kafka, MongoDB, Elasticsearch, and PySpark. Streams raw data from Kafka, enriches it with sentiment analysis using Hugging Face models, stores results in MongoDB, and visualizes data in Elasticsearch with Kibana. Scalable solution for real-time data analytics and machine learning.

This project demonstrates an end-to-end real-time data pipeline designed to perform sentiment analysis on YELP reviews using a combination of Confluent Kafka, Spark Structured Streaming, MongoDB Atlas, HuggingFace's DistilBERT base uncased finetuned SST-2, and Elasticsearch with Kibana for real-time data visualization. The pipeline processes the YELP Dataset (from Kaggle) in real-time and provides step-by-step instructions for building the architecture from scratch.

## System Architecture

![System_architecture.png](final_yelp_overview2.jpg)


## The architecture consists of the following components:

- **Kafka Producer (Kaggle Notebook):** Streams YELP data in real-time from a CSV file.
- **Apache Spark Structured Streaming:** Processes and transforms data in real-time using Spark.
- **MongoDB Atlas:** Serves as an intermediary storage layer for holding processed data.
- **Confluent Kafka: Manages data ingestion and stream processing.
- **HuggingFace Sentiment Model:** DistilBERT base uncased finetuned SST-2 performs sentiment analysis on the reviews.
- **Elasticsearch:** Stores and indexes the data for efficient search and visualization.
- **Kibana:** Provides real-time visualization dashboards for exploring processed data.


## Technologies

- **Python:** Used to develop the Kafka producer, Spark stream processor, and data analysis scripts.
- **Apache Kafka (Confluent Cloud):** Handles data ingestion and message brokering.
- **Apache Spark:** Used for real-time data processing.
- **MongoDB Atlas:** Temporary storage for streaming data.
- **HuggingFace Model:** Performs sentiment analysis on incoming reviews.
- **Elasticsearch:** Stores, indexes, and searches the processed data.
- **Kibana:** Used for building dashboards and visualizing the data.

## What You'll Learn

- How to stream data in real-time using Kafka.
- Setting up a data processing pipeline with Apache Spark.
- Using HuggingFace’s DistilBERT model for sentiment analysis on streaming data.
- Integrating MongoDB Atlas as a temporary data store.
- Ingesting and indexing data into Elasticsearch and creating real-time visualizations in Kibana.
- Working with Kafka connectors (Source & Sink).

## Getting Started
- To get started, clone this repository and follow the detailed steps in the notebooks and configuration files provided.

```git bash git clone git clone https://github.com/akarce/real-time-data-pipeline-kafka-mongo-elasticsearch-pyspark.git```

For a detailed guide and hands-on example, watch the video here: \
[Building a Real-Time Data Streaming Pipeline | End to End Project with Kafka Spark and Elasticsearch
](https://youtu.be/RQ7nnobb1N0)
