#!/usr/bin/env python
# coding: utf-8

# In[2]:


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
warnings.filterwarnings(action = 'ignore') 
import gensim 
from gensim.models import Word2Vec,Phrases
nltk.download('wordnet')


# In[3]:


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
    Key = 'twitter/streamed (3).csv'
)
    
# Read data from the S3 object
data = pd.read_csv(obj['Body'])


# In[ ]:


########################
# Remove punctuation
data['text_processed'] = data['text'].map(lambda x: re.compile(r'[^A-Za-z\s]').sub(" ", x))

# Convert the titles to lowercase
data['text_processed'] = data['text_processed'].map(lambda x: x.lower())
stop_words = stopwords.words('english')

def sent_to_words(sentences):
    for sentence in sentences:
        # deacc=True removes punctuations
        yield(gensim.utils.simple_preprocess(str(sentence), deacc=True))
def remove_stopwords(texts):
    return [[word for word in simple_preprocess(str(doc)) 
             if word not in stop_words] for doc in texts]
words = data.text_processed.values.tolist()
data_words = list(sent_to_words(words))
# remove stop words
data_words = remove_stopwords(data_words)

##########################
# Create Dictionary
id2word = corpora.Dictionary(data_words)
# Create Corpus
texts = data_words
# Term Document Frequency
corpus = [id2word.doc2bow(text) for text in texts]
# View
print(corpus[:1][0][:30])


# In[29]:



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


# In[30]:


# Create Dictionary
id2word = corpora.Dictionary(data_words)
# Create Corpus
texts = data_words
# Term Document Frequency
corpus = [id2word.doc2bow(text) for text in texts]


# In[31]:


# number of topics
import os
num_topics = 10
# Build LDA model
lda_model = gensim.models.LdaMulticore(corpus=corpus,
                                       id2word=id2word,
                                       num_topics=num_topics)
# Print the Keyword in the 10 topics
pprint(lda_model.print_topics())
doc_lda = lda_model[corpus]


# In[55]:


# 1. Wordcloud of Top N words in each topic
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

topics = lda_model.show_topics(formatted=False)

fig, axes = plt.subplots(2, 5, figsize=(10,10), sharex=True, sharey=True)

for i, ax in enumerate(axes.flatten()):
    fig.add_subplot(ax)
    topic_words = dict(topics[i][1])
    cloud.generate_from_frequencies(topic_words, max_font_size=300)
    plt.gca().imshow(cloud)
    plt.gca().set_title('Topic ' + str(i), fontdict=dict(size=16))
    plt.gca().axis('off')


plt.subplots_adjust(wspace=0, hspace=0)
plt.axis('off')
plt.margins(x=0, y=0)
plt.tight_layout()
#fig=plt.show()


# In[54]:


plt.savefig('fig.png')


# In[43]:


import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import pandas as pd
from dash.dependencies import Input, Output
import requests
from plotly.subplots import make_subplots
from datetime import datetime


# In[41]:


import pyLDAvis.gensim_models as gensim_models
pyLDAvis.enable_notebook()
vis = gensim_models.prepare(lda_model, corpus, id2word)
vis


# In[57]:


pyLDAvis.save_html(vis,'lda.html')


# In[ ]:




