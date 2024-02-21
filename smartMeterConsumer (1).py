import os
from google.cloud import pubsub_v1      #pip install google-cloud-pubsub

project_id = "analog-premise-414918"
subscription_id = "smartMeterConsumer"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

import json
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    #print(f"Received {json.loads(message)}.")
    print(f"Received {json.loads(message.data)}.")
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

with subscriber:
    streaming_pull_future.result()
