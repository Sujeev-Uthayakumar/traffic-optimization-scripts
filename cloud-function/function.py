import functions_framework
from google.cloud import pubsub_v1
import base64
import json

# Initialize the Publisher client
publisher = pubsub_v1.PublisherClient()

# Set your Google Cloud project ID and Pub/Sub topic name
PROJECT_ID = 'cloud-final-418702'  # Replace with your project ID
TOPIC_NAME = 'highd-topic'  # Replace with your Pub/Sub topic name
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def publish_message(cloud_event):
    """
    Background Cloud Function to be triggered by Cloud Storage.
    This function is triggered by any file upload to a specified bucket.
    Processes only CSV files.

    Args:
        cloud_event: The CloudEvent payload.
    """
    event = cloud_event.data
    file_name = event['name']
    
    # Check if the file is a CSV by looking at its extension
    if not file_name.endswith('.csv'):
        print(f"Skipping non-CSV file: {file_name}")
        return

    file_data = {
        'bucket': event['bucket'],
        'name': file_name,
        'metageneration': event['metageneration'],
        'timeCreated': event['timeCreated'],
        'updated': event['updated']
    }

    print(f"Processing CSV file: {file_data['name']} from bucket: {file_data['bucket']}.")

    # Convert the file data to JSON
    message_json = json.dumps(file_data)
    message_bytes = message_json.encode('utf-8')

    # Publish a message to the specified Pub/Sub topic
    try:
        publish_future = publisher.publish(topic_path, data=message_bytes)
        publish_future.result()  # Wait for publish to complete.
        print(f"Message published to {TOPIC_NAME}.")
    except Exception as e:
        print(f"An error occurred: {e}")
        raise
