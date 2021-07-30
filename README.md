# Streaming Data Analysis

This is an ongoing project using live data from Twitter API. It's based on Kafka, Spark, Airflow and AWS.

![diagram](https://user-images.githubusercontent.com/65391883/122473883-0ddcad00-cf88-11eb-9ad7-d41521b8e2b5.jpg)

## Collecting and Preprocessing 
### Kafka
#### Creating topic on kafka
Input in command lines:
bin/kafka-topics.sh 
--create 

--zookeeper localhost:2181 

--replication-factor 1 

--partitions 2 

--topic demo-3-twitter

Files for configuration and connecting Twitter API with tweepy. (see the kafka-Proder.py above)
Streaming result sample:
![s1](https://user-images.githubusercontent.com/65391883/126735637-a27106e6-32ac-4df9-9541-96dd45fe8578.png)


### Spark
Read streaming data from Kafka(loading with transformed schema), then use window-operation and spark-sql functions todo preprocessing. Then store streamed structured data in S3. See ETL.py for code.

## Storation
Because of the high frequency of access and uploading in streaming, use Delta Table storation in AWS S3. You can refer to official document: https://docs.delta.io/latest/delta-storage.html

## Analysis
See lda-pyspark.py for code.
![Screen Shot 2021-07-10 at 7 35 18 PM](https://user-images.githubusercontent.com/65391883/125179683-19517b80-e1b6-11eb-914f-c770f66c4b85.png)

1. Working on developing dashboards to visulize:
      a. Barplot of top hashtags
      b. Map of area-hashtag
2. Group the texts with LDA topic analysis.
   
   Visulization examples:
   
   WordCloud:
   
   ![fig](https://user-images.githubusercontent.com/65391883/121825072-1a09f700-cc76-11eb-91eb-3c7354edccd7.png)
   
   With pyLDAvis:
   
   You can see the rankings of topics in all documents, and click the topic number in the left to see words in topic.
   ![Screen Shot 2021-06-13 at 5 22 45 PM](https://user-images.githubusercontent.com/65391883/121825105-59384800-cc76-11eb-8715-cd5e2a6c2c09.png)
   ![Screen Shot 2021-06-13 at 5 23 10 PM](https://user-images.githubusercontent.com/65391883/121825106-5c333880-cc76-11eb-8816-e2b1afc49e47.png)


## Pipeline construction
There are several ways to deploy the above tasks:
1. Deploy with Airflow(cons: inconvenient to tune hyper-parameters) (see dag1.py for code)
2. Deploy all the tasks on AWS, services include S3(for storage), Sagemaker(for ML), Kinesis(to replace Kafka), EC2(schedule and compute) 

