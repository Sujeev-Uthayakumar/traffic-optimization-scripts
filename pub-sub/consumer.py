from google.cloud import pubsub_v1
import json
import time
import os

# Set environment variables for your Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./cloud-final-418702-1f10ada7621d.json"

# Set environment variables for your Google Cloud credentials
project_id = "cloud-final-418702"
subscription_name = "highd-subscription"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)

def callback(message):
    data = json.loads(message.data)
    print(f"Received message: {data}")
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

print("Listening for messages...")

with subscriber:
    streaming_pull_future.result()