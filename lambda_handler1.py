from __future__ import print_function
import json
import time

import boto3
import vaderSentiment.vaderSentiment as vader

print('Loading function')
analyzer = vader.SentimentIntensityAnalyzer()


#This method calls lambda_handler2
def call_lambda_handler2(data):
    client = boto3.client("lambda")
    response = client.invoke(
        FunctionName='lambda_handler2',
        InvocationType='RequestResponse',
        Payload=json.dumps(data),
    )
    res = response['Payload']
    return json.load(res)


# This methods takes a tweet as argument an returns its sentiment
def get_sentiment(tweet):
    scores = analyzer.polarity_scores(tweet['text'])
    compound = scores['compound']

    if compound < -0.05:
        sentiment = "Negative"
        score = scores["neg"]
    elif -0.05 < compound < 0.05:
        sentiment = "Neutral"
        score = scores["neu"]
    else:
        sentiment = "Positive"
        score = scores["pos"]

    tweet_sentiment = {
        "id": tweet['id'],
        "sentiment": sentiment,
        "score": score,
    }
    return tweet_sentiment


def lambda_handler(event, context):
    start_time = time.time()
    to_save = {}
    for k, value in list(event["data"].items()):
        try:
            to_save[k] = get_sentiment(value)
            message = "Execution succeeded"
        except Exception as e:
            message = str(e)
            break
    event["data"] = to_save
    sentiment_time = time.time() - start_time
    res_lh1 = {"time": sentiment_time,
               "message": message}
    res_lh2 = call_lambda_handler2(event)
    res = {"lh1": res_lh1,
           "lh2": res_lh2}
    return res
