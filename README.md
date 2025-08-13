# Word2Stocks

Word2Stocks is a data engineering project focused on building a scalable, cloud-native pipeline for ingesting, processing, and analyzing news and social media data to assess sentiment and bias towards publicly traded companies.

## Project Overview

Our goal is to explore and leverage modern data engineering tools along with cloud services to:

- **Ingest** data from diverse sources such as newspapers, Twitter, Reddit, and potentially company reports.
- **Streamline** data ingestion using AWS Kinesis Data Streams for real-time processing.
- **Pre-process** data with Apache Flink, including:
  - Ticker mapping
  - URL deduplication
  - N-minute window aggregation
  - Language filtering
- **Store** raw and processed data in Amazon S3, potentially using Kinesis Firehose for seamless delivery.
- **Analyze** text using word embeddings to evaluate sentiment and bias towards companies, utilizing AWS Lambda or AWS Glue for scalable compute.
- **Index** and store vectorized data in Amazon OpenSearch for efficient search and retrieval.
- **Build an interface** for querying and visualizing results, possibly leveraging Amazon SageMaker for advanced analytics and model deployment.

## Key Technologies

- **AWS Kinesis Data Streams**: Real-time data ingestion
- **Apache Flink**: Stream processing and enrichment
- **Amazon S3 & Kinesis Firehose**: Scalable storage
- **AWS Lambda / Glue**: Serverless data transformation
- **Amazon OpenSearch**: Vector search and analytics
- **Amazon SageMaker**: Machine learning and interface (optional)

## Vision

By integrating these technologies, Word2Stocks aims to provide a robust platform for real-time financial news analysis, supporting research and decision-making in the finance sector.
