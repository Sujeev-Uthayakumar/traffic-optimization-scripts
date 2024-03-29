from google.cloud import storage, pubsub_v1
import pandas as pd
import os
import csv
import json

# Set environment variables for your Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./cloud-final-418702-1f10ada7621d.json"

# Google Cloud Storage and Pub/Sub Configuration
bucket_name = 'highd-dataset-final'
topic_name = 'projects/cloud-final-418702/topics/highd-topic'

# Initialize Google Cloud Storage and Pub/Sub clients
storage_client = storage.Client()
pubsub_publisher = pubsub_v1.PublisherClient()
bucket = storage_client.bucket(bucket_name)

def publish_message_to_pubsub(data):
    """Publishes a message to a Pub/Sub topic."""
    # Data must be a bytestring
    data = data.encode("utf-8")
    # When you publish a message, the client returns a future.
    future = pubsub_publisher.publish(topic_name, data)
    print(f"Published messages to {topic_name} with message ID: {future.result()}")

def ingest_csv_from_gcs_to_pubsub():
    # Call a method that returns an HTTPIterator object
    blobs = bucket.list_blobs()

    # Iterate over the HTTPIterator object to access the items
    for blob in blobs:
        temp_blob = bucket.blob(blob.name)
        # Download the blob contents as a bytes object
        blob_data = temp_blob.download_as_bytes()

        # Convert the bytes data to a string and split into lines
        csv_data = blob_data.decode('utf-8').splitlines()

        reader = csv.reader(csv_data)
        for row in reader:
            print(row)
            publish_message_to_pubsub(json.dumps(row))
            
if __name__ == "__main__":
    ingest_csv_from_gcs_to_pubsub()