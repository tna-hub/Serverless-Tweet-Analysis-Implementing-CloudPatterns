from __future__ import print_function
import json
import boto3
import vaderSentiment.vaderSentiment as vader

print('Loading function')
analyzer = vader.SentimentIntensityAnalyzer()


def call_lambda_handler2(data):
    client = boto3.client("lambda")
    response = client.invoke(
        FunctionName='lambda_handler2',
        InvocationType='RequestResponse',
        Payload=json.dumps(data),
    )
    return str(response)


def get_sentiment(tweet):
    scores = analyzer.polarity_scores(tweet['text'])
    score = None
    compound = scores['compound']
    sentiment = None

    if compound < -0.05:
        sentiment = "Negative"
        score = scores["neg"]
    elif -0.05 < compound < 0.05:
        sentiment = "Neutral"
        score = scores["neu"]
    else:
        sentiment = "Positive"
        score = scores["pos"]

    tweet_sentiment = {"sentiment": sentiment,
                       "score": score,
                       "id": tweet['id'],
                       "date": tweet['date'],
                       "text": tweet['text']
                       }
    return tweet_sentiment


def lambda_handler(event, context):
    print(event)
    to_save = {}
    res = to_save
    for k, value in list(event.items()):
        to_save[k] = get_sentiment(value)
    call_lambda_handler2(to_save)
    return to_save
    raise Exception('Something went wrong')
