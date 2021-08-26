# Streaming Data Analysis

This is a project using live data from Twitter API, based on Kafka, Spark, Airflow and AWS. Frequency of triggering is 5mins/day.

![Screen Shot 2021-08-26 at 12 13 28 AM](https://user-images.githubusercontent.com/65391883/130904760-f497998a-e3c1-458a-b8a5-17ebee03d024.png)


## Collecting and Preprocessing 
### Kafka
![Screen Shot 2021-08-26 at 12 35 50 AM](https://user-images.githubusercontent.com/65391883/130906724-46c139ef-f4ad-4c6c-80da-f16cf2c71a2d.png)

Streaming result sample:
![s1](https://user-images.githubusercontent.com/65391883/126735637-a27106e6-32ac-4df9-9541-96dd45fe8578.png)


### Spark
![Screen Shot 2021-08-26 at 12 42 15 AM](https://user-images.githubusercontent.com/65391883/130907275-85ee9c9c-9250-4d65-88f8-685543c967f1.png)

See ETL.py for code.

## Storation
Because of the high frequency of access and uploading in streaming, use Delta Table storation in AWS S3. 

## Analysis


See lda-pyspark.py for code.

1. Visulize barplot of top hashtags
![Screen Shot 2021-07-10 at 7 35 18 PM](https://user-images.githubusercontent.com/65391883/125179683-19517b80-e1b6-11eb-914f-c770f66c4b85.png)
      
2. Group the texts with LDA topic analysis.
   Firstly find out the hyperparameter with cross validation, then pass it to full dataset with Xcom.
   
   Visulization examples:
   
   With pyLDAvis:
   
   You can see the rankings of topics in all documents, and click the topic number in the left to see words in topic.
   ![Screen Shot 2021-06-13 at 5 22 45 PM](https://user-images.githubusercontent.com/65391883/121825105-59384800-cc76-11eb-8715-cd5e2a6c2c09.png)
   ![Screen Shot 2021-06-13 at 5 23 10 PM](https://user-images.githubusercontent.com/65391883/121825106-5c333880-cc76-11eb-8816-e2b1afc49e47.png)


## Pipeline construction
Deploy with Airflow(cons: inconvenient to tune hyper-parameters) (see dag_twitter.py for code)
