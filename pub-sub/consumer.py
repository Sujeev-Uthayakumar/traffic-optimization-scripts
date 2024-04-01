from google.cloud import pubsub_v1
import json
import time
import os

# Set environment variables for your Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./cloud-final-418702-1f10ada7621d.json"

# Set environment variables for your Google Cloud credentials
project_id = "cloud-final-418702"
subscription_name = "highd-processed-topic-sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)

def callback(message):
    try:
        # Attempt to decode message data as UTF-8
        message_text = message.data.decode('utf-8')
        try:
            # Attempt to load the message text as JSON
            data = json.loads(message_text)
            print(f"Received message: {data}")
        except json.JSONDecodeError:
            # If JSON decoding fails, log the text as-is
            print(f"Received non-JSON message: {message_text}")
    except UnicodeDecodeError:
        # If UTF-8 decoding fails, handle binary data differently
        print("Received message with binary data; cannot decode as UTF-8.")
        # Optionally, you can print out the binary data in hex or base64 for inspection
        # For example, to print the data in base64:
        import base64
        print(f"Binary data (base64): {base64.b64encode(message.data)}")
    
    message.ack()


streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

print("Listening for messages...")

with subscriber:
    streaming_pull_future.result()