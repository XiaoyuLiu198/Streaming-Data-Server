#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import boto3
import io
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import pandas as pd
from dash.dependencies import Input, Output
import requests
from plotly.subplots import make_subplots
from datetime import datetime


# In[ ]:


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


#-------------------------------------------------------------------------------

def update_twitter():
    #access structure data from S3(this part will be uploaded afterwards)
    aws_id = os.environ['AWS_ID']
    aws_secret = os.environ['AWS_SECRET']
    #bucket store full data set
    my_bucket1 = 'xxxxxx'
    #bucket store summarized hashtag info(dealed with late-data in spark)
    my_bucket2 = 'xxxxxx'
    object_key = 'xxxxxx'
    get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))

    s3 = boto3.client('s3', aws_access_key_id=aws_id,
            aws_secret_access_key=aws_secret)
    objs1 = s3.list_objects_v2(Bucket='my_bucket1')['Contents']
    objs2 = s3.list_objects_v2(Bucket='my_bucket2')['Contents']
    fullSet = pd.read_csv(io.BytesIO(obj['object_key'] for obj in sorted(objs1, key=get_last_modified)[0])
    hashtags_df=pd.read_csv(io.BytesIO(obj['object_key'] for obj in sorted(objs2, key=get_last_modified)[0])
    
    def convertTime(t):
        t = int(t)
        return datetime.fromtimestamp(t)

    fullSet = fullSet.dropna(subset=["hashtag"])
    fullSet["location"].fillna(value="", inplace=True)

    fullSet["created_time"]= fullSet["created_time"]/1000
    fullSet["created_time"] = fullSet["created_time"].apply(convertTime)
    fullSet=fullSet.groupby(["location"], as_index=False).agg(
    {
        "id" : "count"
    }
)
    fullSet.columns=['twitter_num']                       
    fullSet_for_map = fullSet.groupby(["location","hashtags"], as_index=False).agg(
    {
        "id" : "count"
    }
)
    fullSet_for_map.columns=['num_in_hashtag']
                            
    top10_hashtags = hashtags_df["hashtags"].tolist()
    top10_num = hashtags_df["nums"].tolist()
                            
    ##figure producing
    fig = make_subplots(
        rows = 2, cols = 4,
        specs=[
                [{"type": "scattergeo", "rowspan": 4, "colspan": 3}, None, None ],
                [    None, None, None,               {"type": "bar", "colspan":3}, None, None],
              ]
    )
    #notes
    message = fullSet_for_map["twitter_sum"].astype(str) + "<br>"
    message += "Twits in " + fullSet_for_map["location"]
    fullSet_for_map["text"] = message
    #make map
    fig.add_trace(
        go.Scattergeo(
            lon = fullSet_for_map["Long_"],
            lat = fullSet_for_map["Lat"],
            hovertext = fullSet_for_map["text"],
            showlegend=False,
            marker = dict(
                size = 5,
                opacity = 0.8,
                reversescale = True,
                autocolorscale = True,
                symbol = 'square',
                line = dict(
                    width=1,
                    color='rgba(103, 102, 102)'
                ),
                cmin = 0,
                color = fullSet_for_map['twitter_num'],
                cmax = fullSet_for_map['num_in_hashtag'].max(),
                colorbar_title="Number of Twits in this location",  
                colorbar_x = -0.05
            )

        ),

        row=1, col=1
    )
    
    fig.add_trace(
        go.Bar(
            x=top10_hashtags,
            y=top10_num, 
            name= "Twits with Hashtag",
            marker=dict(color="Yellow"), 
            showlegend=True,
        ),
        row=2, col=4
    )

    
                            
    fig.update_layout(
        template="plotly_dark",
        title = "Global Twitter Hashtags (Last Updated: " + str(df_final["Last_Update"][0]) + ")",
        showlegend=True,
        legend_orientation="h",
        legend=dict(x=0.65, y=0.8),
        geo = dict(
                projection_type="orthographic",
                showcoastlines=True,
                landcolor="white", 
                showland= True,
                showocean = True,
                lakecolor="grey"
        )
    )
    fig


    return ([html.Div(children='''
        Live data dashboard for twitter
    '''),

    dcc.Graph(
        id='example-graph',
        figure=fig
    )
        ])

#-------------------------------------------------------------------------------
app.layout = html.Div([
    dcc.Interval(
                id='my_interval',
                disabled=False,     #if True the counter will no longer update
                n_intervals=0,      #number of times the interval has passed
                interval=150*1000,  #increment the counter n_intervals every 5 minutes
                max_intervals=100,  #number of times the interval will be fired.
                                    #if -1, then the interval has no limit (the default)
                                    #and if 0 then the interval stops running.
    ),
    html.Div([
        html.Div(id="twitter_live", children=update_twitter()
        )
    ]),
    ])

#-------------------------------------------------------------------------------
# Callback to update news
@app.callback(Output("twitter_live", "children"), [Input("my_interval", "n_intervals")])
def update_twitter_div(n):
    return update_twitter()
#-------------------------------------------------------------------------------
if __name__ == '__main__':
    app.run_server(debug=False)


# In[ ]:




