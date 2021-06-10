# Streaming Data Analysis

This is an ongoing project using live data from Twitter API. I tried several ways to process data using Spark(in scala), pyspark(using python), tweepy(using python) and Kafka to collect data. I have finished the collection and transformation process. 

## Collecting and Preprocessing 
### Kafka
#### Creating topic on kafka
Input in command lines:
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic example-topic

Files for configuration and connecting with Twitter API (see the files above)

### Spark
Read streaming data from Kafka with readStream -- Preprocessing -- Execute the query with writeStream -- Store streamed structured data in S3.

## Storation
Because of the high frequency of access and uploading, use Delta Table storation in AWS S3.

## Analysis
1. Working on developing dashboards to visulize:
      a. Barplot of top hashtags
      b. Map of area-hashtag
2. Working on grouping the hashtags with LDA topic analysis.

## Pipeline construction
There are several ways to deploy the above tasks:
1. Deploy with Airflow(cons: inconvenient to tune hyper-parameters)
2. Deploy all the tasks on AWS, services include S3(for storage), Sagemaker(for ML), Kinesis(to replace Kafka), EC2(schedule and compute) 

