import json
import os
import pandas as pd
from confluent_kafka import Producer

conf = {
    "bootstrap.servers":os.getenv("BOOTSTRAP_SERVERS"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.getenv("SASL.USERNAME"),
    "sasl.password":os.getenv("SASL.PASSWORD"),
    "client.id": "json-serial-producer"
}

producer = Producer(conf)

topic = "raw-topic"

def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered successfully! Key: {msg.key()}")


def read_checkpoint(checkpoint_file):
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as file:
            return int(file.read().strip())
    return 0
    
def write_checkpoint(checkpoint_file, index):
    with open(checkpoint_file, "w") as file:
        file.write(str(index))
    print(f"Checkpoint updated to line: {index}")

def handle_date(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    raise TypeError(f"Object of type {type(obj).__name__} is not serialisable")

## Main function
def stream_json_serially(file_path, checkpoint_file='/kaggle/working/checkpoint.txt'):
    last_sent_index = read_checkpoint(checkpoint_file)
    with open(file_path, "r") as file:
        for idx, line in enumerate(file):
            if idx < last_sent_index:
                continue

            try:
                record = json.loads(line)
                producer.produce(
                        topic,
                        key = str(record["review_id"]),
                        value = json.dumps(record, default=handle_date).encode("utf-8"),
                        callback=delivery_report
                )
                producer.flush()
                write_checkpoint(checkpoint_file, idx+1)
            except json.JSONDecodeError as e:
                print("Failed to decode JSON: {e}")

if __name__=="__main__":
     stream_json_serially("/kaggle/input/yelp-dataset/yelp_academic_dataset_review.json")


    
