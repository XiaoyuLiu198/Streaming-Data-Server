# Streaming Data Analysis Spark+Kafka

This is an ongoing project using live data from Twitter API. I tried several ways to process data using Spark(in scala), pyspark(using python), tweepy(using python) and Kafka.

## Collecting and Preprocessing 
### Kafka
#### Creating topic on kafka
Input in command lines:
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic example-topic

Java files for configuration and connecting with Twitter API (see the files above)

### Spark
Read streaming data from Kafka with readStream -- Preprocessing -- Execute the query with writeStream


Working on: 

  Further analysis with Spark Mlib.
  
  Display with python dash.
