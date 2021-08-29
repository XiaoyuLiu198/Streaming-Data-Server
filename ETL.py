#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import json
import pyspark.sql.functions as fn
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from delta.table import *
from time import sleep

# In[ ]:


import os
os.environ['HADOOP_HOME'] = "####"
print(os.environ['HADOOP_HOME'])


# In[ ]:


spark = SparkSession     .builder     .appName("twitter")     .master("local[*]")     .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4')     .getOrCreate()
hadoop_conf=spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
hadoop_conf.set("fs.s3n.awsAccessKeyId", aws_key)
hadoop_conf.set("fs.s3n.awsSecretAccessKey", aws_secret)
conf = spark.sparkContext._conf.setAll([('spark.delta.logStore.class','org.apache.spark.sql.delta.storage.S3SingleDriverLogStore')])
spark.sparkContext._conf.getAll()
# In[ ]:


df = spark     .readStream     .format("kafka")     .option("kafka.bootstrap.servers", "localhost:9092")     .option("subscribe", "demo-3-twitter")     .option("startingOffsets", "latest")     .load()

struct_type_mapping = {
    "int32": IntegerType,
    "int64": LongType,
    "string": StringType,
    "boolean": BooleanType,
}


def transform_schema_kafka_to_spark(schema: dict) -> StructType:
    """Assume that schema is a nested schema (its type is `struct`) """
    final_schema = StructType()
    for field in schema["fields"]:
        if field["type"] == "struct":
            final_schema.add(field["field"],
                             data_type=transform_schema_kafka_to_spark(field),
                             nullable=field["optional"])
        elif field["type"] == "array":
            final_schema.add(field["field"],
                             data_type=ArrayType(transform_schema_kafka_to_spark(field["items"])),
                             nullable=field["optional"])
        else:
            final_schema.add(field["field"],
                             data_type=struct_type_mapping[field['type']](),
                             nullable=field['optional'])
    return final_schema


onlyValue = df.selectExpr("CAST(value AS STRING)").writeStream.format("memory").queryName("onlyValue").start()
onlyValue.awaitTermination(timeout=10)

sample = spark.table("onlyValue")

kafka_schema = json.loads(sample.head().asDict()["value"])["schema"]
spark_schema = transform_schema_kafka_to_spark(kafka_schema)
schema = StructType().add("payload", spark_schema).add("schema", StructType())

data = df.select(fn.from_json(df["value"].cast("string"), schema).alias("value")).alias("data")
data.printSchema()

hashtags = data.select(fn.explode("data.value.payload.entities.hashtags").alias("hashtag"),
                       fn.col("data.value.payload.coordinates").alias("coordinates"),
                       fn.col("data.value.payload.user.location").alias("location_name"),
                       fn.col("data.value.payload.text").alias("text"),
                       fn.to_timestamp("data.value.payload.created_at").alias("created_time")) \
    .select(fn.lower(fn.col("hashtag.text")).alias("hashtag"), "created_at")

hashtagCount = hashtags.groupBy(fn.window(hashtags["created_at"], "10 minutes", "5 minutes"), "hashtag")     .count().orderBy(["window", "count"], ascending=[False, False])

query = hashtagCount.writeStream.outputMode("append").format("delta").trigger(Trigger.ProcessingTime("300 seconds")).option('checkpointLocation', checkpoint_location).start()
sleep(600)
query.stop()




