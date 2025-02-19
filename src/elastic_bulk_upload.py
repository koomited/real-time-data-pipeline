from elasticsearch import Elasticsearch, helpers
import json, os

## Connect to Elasticsearch cloud
client = Elasticsearch(
    os.getenv("ELASTIC.URI"),
    verify_certs = False,
    basic_auth = ("elastic", os.getenv("ELASTIC.PASSWORD"))
)

# Open yelp data file
with open("/kaggle/input/yelp-dataset/yelp_academic_dataset_business.json") as f:
    data  = []
    for line in f:
        try:
            doc = json.loads(line.strip())
            data.append(doc)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
    helpers.bulk(client, data, index="businesses")
            