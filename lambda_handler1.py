from __future__ import print_function
import boto3
import json

def call_lambda_handler2(data):
    client = boto3.client("lambda")
    response = client.invoke(
        FunctionName='lambda_handler2',
        InvocationType='RequestResponse',
        Payload=json.dumps(data),
    )
    return str(response)


def lambda_handler1(event, context):
    if event:
        # extracting event records
        data = event["Records"][0]