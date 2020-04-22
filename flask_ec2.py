from __future__ import print_function
import subprocess
import sys
import time

# Initialisation
def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])


pkgs = ['flask', 'boto3']
for pkg in pkgs:
    install(pkg)

from flask import Flask
from flask import request
from flask import jsonify
import boto3
import json


app = Flask(__name__)

"""This method is calling another Lambda function"""
def call_lambda_handler1(data):
    print(data)
    client = boto3.client("lambda")
    response = client.invoke(
        FunctionName='lambda_handler1',
        InvocationType='RequestResponse',
        LogType='None',
        Payload=json.dumps(data),
    )
    return json.load(response['Payload'])

def preprocess(data):
    for k, value in list(data.items()):
        #Remove non ascii characters from tweets
        value["text"] = value["text"].encode("ascii", "ignore").decode()
        tid = value['id']
        date = value['date']
        #Delete tweets without idea or date
        if tid is None or date is None:
            print("Deleted the tweet with id {} and date {}".format(tid, date))
            del data[k]
    return data

@app.route('/sentiment', methods=["POST", "GET"])
def process():
    if request.method != "POST":
        return "To access this service send a POST request to this URL with" \
               " the text you want analyzed in the body."
    else:
        data = request.get_json(force=True)
        #Saving the start time
        start_time = time.time()
        data = preprocess(data)
        response1 = call_lambda_handler1(data)
        print(response1)
        res = {"Total time": time.time() - start_time,
               "response": response1}
        return jsonify(res)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
