#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import re
import gensim
from gensim.utils import simple_preprocess
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
import gensim.corpora as corpora
from pprint import pprint
#import pyLDAvis.gensim
import pickle 
import pyLDAvis
import pandas as pd
import boto3
import warnings
import re
import seaborn as sns
import matplotlib.pyplot as plt
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import sent_tokenize, word_tokenize 
from sklearn.datasets import make_classification
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import RepeatedStratifiedKFold
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
warnings.filterwarnings(action = 'ignore') 
# Creating the low level functional client
client = boto3.client(
    's3',
    aws_access_key_id = 'XXX',
    aws_secret_access_key = 'XXX',
    region_name = 'us-east-2'
)
    
# Creating the high level object oriented interface
resource = boto3.resource(
    's3',
    aws_access_key_id = 'XXX',
    aws_secret_access_key = 'XXX',
    region_name = 'us-east-2'
)
##read from key in buckets
obj = client.get_object(
    Bucket = 'XX',
    Key = last_added
)
get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
objs = client.list_objects_v2(Bucket='twitter')['Contents']
last_added = [obj['Key'] for obj in sorted(objs, key=get_last_modified)][0]

# Read data from the S3 object
data = pd.read_csv(io.BytesIO(obj.get()['Body']))
dataSample=data['text'].sample(n=8000)
def clean_review(comments: str) -> str:
    #Remove non-letters
    letters_only = re.compile(r'[^A-Za-z\s]').sub(" ", comments)
    #Convert to lower case
    lowercase_letters = letters_only.lower()
    return lowercase_letters


def lemmatize(tokens: list) -> list:
    #Lemmatize
    tokens = list(map(WordNetLemmatizer().lemmatize, tokens))
    lemmatized_tokens = list(map(lambda x: WordNetLemmatizer().lemmatize(x,"v"), tokens))
    #Remove stop words
    meaningful_words = list(filter(lambda x: not x in en_stops, lemmatized_tokens))
    return meaningful_words


def preprocess(review: str, total: int, show_progress: bool = True) -> list:
    if show_progress:
        global counter
        counter += 1
        print('Processing... %6i/%6i'% (counter, total), end='\r')
    review = clean_review(review)
    tokens = word_tokenize(review)
    lemmas = lemmatize(tokens)
    return lemmas
counter=0
en_stops = stopwords.words('english')
##extra stopping words
extra_stops=['http','u','rt','co','get']
for word in extra_stops:
    en_stops.append(word)
data_words=list(map(lambda x: preprocess(x,len(data['text'])),data['text']))
vectorizer = CountVectorizer(analyzer='word',       
                             min_df=10,                        
                             stop_words='english',             
                             lowercase=True,                   
                             token_pattern='[a-zA-Z0-9]{3,}',            
                            )

data_vectorized = vectorizer.fit_transform(data_words)
data_dense = data_vectorized.todense()
model = LinearDiscriminantAnalysis()
# define model evaluation method
cv = RepeatedStratifiedKFold(n_splits=10, n_repeats=3, random_state=1)
# define grid
grid = dict()
grid['solver'] = ['svd', 'lsqr', 'eigen']
grid['n_components']=[5,7,9,11]

# define search
search = GridSearchCV(model, grid, scoring='accuracy', cv=cv, n_jobs=3)
# perform the search
results = search.fit(X, y)
numOfTopics=result.best_estimator_['n_components']
task_instance = kwargs['task_instance']
task_instance.xcom_push(key='numOfTopics', value=numOfTopics)

