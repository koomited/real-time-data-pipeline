from elasticsearch import Elasticsearch, helpers
import json

## Connect to Elasticsearch cloud
client = Elasticsearch(
    "https://e718e1052e864dcd800993c7c50a3894.eu-central-1.aws.cloud.es.io:443",
    verify_certs = False,
    basic_auth = ("elastic", "hMnAFFFZPflZRGlUYGzsn2gX")
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
            