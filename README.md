# traffic-optimization-scripts

## Overview

This project is designed to ingest, process, store, and analyze vehicle tracking data from the HighD dataset. Using Google Cloud Platform's suite of tools, the data pipeline is structured to handle large-scale traffic data for comprehensive analysis and insights.

## Architecture

The data processing pipeline is composed of the following components:

- **Data Source**: HighD Dataset, a high-resolution traffic dataset that provides detailed vehicle tracking data.
- **Ingest**: Data is ingested into Google Cloud Storage for durable and scalable storage.
- **Cloud Functions**: Triggered on new data upload to Cloud Storage, which then publishes a message to Pub/Sub with the name and location of the new file.
- **Pub/Sub**: Messaging service that decouples services that produce events from services that process events.
- **Dataflow**: Data processing service that reads messages from Pub/Sub, processes the data (e.g., transforms, enriches, cleans data), and outputs to Cloud SQL.
- **Cloud SQL**: Managed relational database service that stores the processed data.
- **BigQuery**: Data warehouse tool for running fast SQL queries on large datasets.
- **Datalab**: An interactive tool for exploration, analysis, visualization, and machine learning.

## Getting Started
To get started with this project, follow these steps:

1. **Set up GCP Environment**: Ensure that all GCP services used in this project are enabled in your Google Cloud project.
2. **Deploy Cloud Function**: Navigate to the `cloud_function/` directory and follow the deployment instructions in the README there.
3. **Deploy Dataflow Pipeline**: Navigate to the `dataflow/` directory and follow the instructions to deploy the pipeline to process your data.
5. **Data Storage**: Ensure your Cloud SQL instance is configured correctly for use by Dataflow.
6. **Data Analysis**: Use BigQuery and Datalab to analyze the processed data. Notebooks and scripts can be found in the `analysis/` directory.

## Dataflow Script
> python dataflow.py \
> --runner DataflowRunner \
> --project cloud-final-418702 \
> --region northamerica-northeast2 \
> --temp_location gs://highd-dataset-final/temp \
> --staging_location gs://highd-dataset-final/staging \
> --streaming \
> --experiment use_unsupported_python_version \
> --requirements_file ./requirements.txt
