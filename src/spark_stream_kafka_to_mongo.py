from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from transformers import pipeline
import logging
import os

logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')



checkpoint_dir = "/kaggle/working/checkpoints/kafka_to_mongo"
if not os.path.exists(checkpoint_dir):
    os.makedirs(checkpoint_dir)

config = {
    "kafka":{
    "bootstrap.servers":os.getenv("BOOTSTRAP_SERVERS"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.getenv("SASL.USERNAME"),
    "sasl.password":os.getenv("SASL.PASSWORD"),
    "client.id": "json-serial-producer"
},

    "mongodb": {
        "uri":os.getenv("MONGO.URI"),
        "database":"reviewsdb",
        "collection": "enriched_reviews_collection"
    }
}

sentiment_pipeline = pipeline("text-classification", model="distilbert/distilbert-base-uncased-finetuned-sst-2-english")

def analyse_sentiment(text):
    if text and isinstance(text, str):
        try:
            result = sentiment_pipeline(text)[0]
            return result["label"]
        except Exception as e:
            logging.error(f"Error in sentiment analysis: {e}")
            return "Error"
    return "Empty or Invalid"

sentiment_udf = udf(analyse_sentiment, StringType())

def read_from_kafka_and_write_to_mongo(spark):
    topic = "raw-topic"
    
    schema = StructType([
        StructField("review_id",StringType()),
        StructField("user_id",StringType()),
        StructField("business_id",StringType()),
        StructField("stars",FloatType()),
        StructField("useful",IntegerType()),
        StructField("funny",IntegerType()),
        StructField("cool",IntegerType()),
        StructField("text",StringType()),
        StructField("date",StringType())
    ])
    stream_df = (spark.readStream
                 .format("kafka")
                 .option("kafka.bootstrap.servers",config['kafka']['bootstrap.servers'])
                 .option("subscribe",topic)
                 .option("kafka.security.protocol", config['kafka']['security.protocol'])
                 .option("kafka.sasl.mechanism",config['kafka']['sasl.mechanisms'])
                 .option("kafka.sasl.jaas.config",
                        f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{config["kafka"]["sasl.username"]}" '
                        f'password="{config["kafka"]["sasl.password"]}";')
                 .option("failOnDataLoss","false")
                 .load()
                )
    parsed_df = stream_df.select(from_json(col('value').cast("string"), schema).alias("data")).select("data.*")
    
    enriched_df = parsed_df.withColumn("sentiment", sentiment_udf(col('text')))
    query = (enriched_df.writeStream
             .format("mongodb")
             .option("spark.mongodb.connection.uri", config['mongodb']['uri'])
             .option("spark.mongodb.database", config['mongodb']['database'])
             .option("spark.mongodb.collection", config['mongodb']['collection'])
             .option("checkpointLocation", checkpoint_dir)
             .outputMode("append")
             .start()
             .awaitTermination()
            )
    print("Done1")
if __name__ == "__main__":
    spark = (SparkSession.builder
          .appName("KafkaStreamToMongo")
          .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0")
          .getOrCreate()
          )
    read_from_kafka_and_write_to_mongo(spark)