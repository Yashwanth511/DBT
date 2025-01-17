Kafka Streaming Application

This repository contains a Kafka-based streaming application built using Python, Apache Kafka, and Apache Spark. The application processes tweets, analyzes hashtags, and categorizes tweets into three sentiment topics: positive, negative, and neutral.

Overview

The project consists of three main components:

Producer: Reads a dataset of tweets and sends them to Kafka topics based on sentiment.

Batch Consumer: Analyzes batch data from a MySQL database and extracts hashtag insights.

Streaming Consumer: Processes real-time Kafka streams and generates hashtag statistics.

Features

Real-time tweet processing using Kafka.

Sentiment-based categorization into positive, negative, and neutral topics.

Hashtag extraction and aggregation.

Batch processing of historical tweet data.

Setup

Prerequisites

Python 3.8+

Apache Kafka

Apache Spark

MySQL Database

Required Python libraries:

pandas

kafka-python

pyspark
