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

![Screen Shot 2021-08-26 at 11 38 58 PM](https://user-images.githubusercontent.com/65391883/131072611-e4cbf615-480c-4f2f-bf48-903bb82eacd1.png)

See lda-pyspark.py for code.

### 1. Visulize barplot of top hashtags
![Screen Shot 2021-07-10 at 7 35 18 PM](https://user-images.githubusercontent.com/65391883/125179683-19517b80-e1b6-11eb-914f-c770f66c4b85.png)
      
### 2. Group the texts with LDA topic analysis.

   Firstly find out the hyperparameter with cross validation, then pass it to full dataset with Xcom. Use sparknlp session and mlib to do LDA analysis. Result is as followed:
   
   ![Screen Shot 2021-08-29 at 9 25 14 PM](https://user-images.githubusercontent.com/65391883/131276753-1c265b7b-76de-4f5d-bcda-68f527e3dc59.png)

   
   Visulization examples:
   
   With pyLDAvis:
   
   You can see the rankings of topics in all documents, and click the topic number in the left to see words in topic.
   ![Screen Shot 2021-08-29 at 8 47 18 PM](https://user-images.githubusercontent.com/65391883/131274346-73f6f0c2-2763-49e8-82bc-a4cd7fb74cb2.png)


## Pipeline construction
Deploy with Airflow(see dag_twitter.py for code)
