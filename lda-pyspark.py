#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from sparknlp.base import *
from sparknlp.annotator import *
from sparknlp.pretrained import PretrainedPipeline
import sparknlp
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
spark = SparkSession.builder     .appName("Spark NLP")    .config("spark.driver.memory","8G")\ #change accordingly
    .config("spark.driver.maxResultSize", "2G")     .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.11:2.4.5")    .config("spark.kryoserializer.buffer.max", "1000M")    .getOrCreate()
###################################################################

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "xxxx")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "xxx")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "us-east-2.amazonaws.com")
get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
file_location_http="xxx"
s3 = boto3.client('s3')
objs = s3.list_objects_v2(Bucket='twitter')['Contents']
last_added = [obj['Key'] for obj in sorted(objs, key=get_last_modified)][0]
df=spark.read.csv(last_added)

# convert input dataframe to document. 
document_assembler = DocumentAssembler()     .setInputCol("headline_text")     .setOutputCol("document")     .setCleanupMode("shrink")
# Split sentence to tokens(array)
tokenizer = Tokenizer()   .setInputCols(["document"])   .setOutputCol("token")
# clean
normalizer = Normalizer()     .setInputCols(["token"])     .setOutputCol("normalized")
# remove stopwords
stopwords_cleaner = StopWordsCleaner()      .setInputCols("normalized")      .setOutputCol("cleanTokens")      .setCaseSensitive(False)
# stem the words to bring them to the root form.
stemmer = Stemmer()     .setInputCols(["cleanTokens"])     .setOutputCol("stem")
# bring back the expected structure viz. array of tokens.
finisher = Finisher()     .setInputCols(["stem"])     .setOutputCols(["tokens"])     .setOutputAsArray(True)     .setCleanAnnotations(False)
# build preprocess pipeline
preprocess_pipeline = Pipeline(
    stages=[document_assembler, 
            tokenizer,
            normalizer,
            stopwords_cleaner, 
            stemmer, 
            finisher])
# train the pipeline
preprocess = preprocess_pipeline.fit(df)
# apply the pipeline to transform dataframe.
processed_df  = preprocess.transform(df)
# select the columns that we need
tokens_df = processed_df.select('publish_date','tokens').limit(10000)
tokens_df.show()

from pyspark.ml.feature import CountVectorizer
cv = CountVectorizer(inputCol="tokens", outputCol="features", vocabSize=500, minDF=3.0)
# train the model
cv_model = cv.fit(tokens_df)
# transform the data. Output column name will be features.
vectorized_tokens = cv_model.transform(tokens_df)

from pyspark.ml.clustering import LDA
num_topics = 10
lda = LDA(k=num_topics, maxIter=10)
model = lda.fit(vectorized_tokens)
ll = model.logLikelihood(vectorized_tokens)
lp = model.logPerplexity(vectorized_tokens)

# extract vocabulary from CountVectorizer
vocab = cv_model.vocabulary
topics = model.describeTopics()   
topics_rdd = topics.rdd
topics_words = topics_rdd       .map(lambda row: row['termIndices'])       .map(lambda idx_list: [vocab[idx] for idx in idx_list])       .collect()

from pyspark.sql.functions import udf, col, size, explode, regexp_replace, trim, lower, lit
from pyspark.sql.types import ArrayType, StringType, DoubleType, IntegerType, LongType
import pyLDAvis

transformed = model.transform(vectorized_tokens)
def format_data_to_pyldavis(df_filtered, count_vectorizer, transformed, lda_model):
    xxx = df_filtered.select((explode(df_filtered.words_filtered)).alias("words")).groupby("words").count()
    word_counts = {r['words']:r['count'] for r in xxx.collect()}
    word_counts = [word_counts[w] for w in count_vectorizer.vocabulary]


    data = {'topic_term_dists': topic_term_dists = np.array([row for row in lda_model.describeTopics(maxTermsPerTopic=len(count_vectorizer.vocabulary)).select(col('termWeights')).toPandas()['termWeights']]), 
            'doc_topic_dists': np.array([x.toArray() for x in transformed.select(["topicDistribution"]).toPandas()['topicDistribution']]),
            'doc_lengths': [r[0] for r in df_filtered.select(size(df_filtered.words_filtered)).collect()],
            'vocab': count_vectorizer.vocabulary,
            'term_frequency': word_counts}

    return data
data = format_data_to_pyldavis(tokens_df, cv_model, transformed, model)
py_lda_prepared_data = pyLDAvis.prepare(**data)
pyLDAvis.display(py_lda_prepared_data)
pyLDAvis.save_html(vis,file_location_http+'lda.html')

# 2. Wordcloud of Top N words in each topic
from matplotlib import pyplot as plt
from wordcloud import WordCloud, STOPWORDS
import matplotlib.colors as mcolors

cols = [color for name, color in mcolors.TABLEAU_COLORS.items()]  # more colors: 'mcolors.XKCD_COLORS'

cloud = WordCloud(stopwords=en_stops,
                  background_color='white',
                  width=2500,
                  height=1800,
                  max_words=10,
                  colormap='tab10',
                  color_func=lambda *args, **kwargs: cols[i],
                  prefer_horizontal=1.0)

fig, axes = plt.subplots(2, 5, figsize=(10,10), sharex=True, sharey=True)

for i, ax in enumerate(axes.flatten()):
    fig.add_subplot(ax)
    topic_words = dict(topics[i][1])
    cloud.generate_from_frequencies(topics_words, max_font_size=300)
    plt.gca().imshow(cloud)
    plt.gca().set_title('Topic ' + str(i), fontdict=dict(size=16))
    plt.gca().axis('off')


plt.subplots_adjust(wspace=0, hspace=0)
plt.axis('off')
plt.margins(x=0, y=0)
plt.tight_layout()
plt.savefig(file_location_http+'fig.png')
