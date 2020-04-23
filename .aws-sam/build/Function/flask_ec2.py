from __future__ import print_function
import subprocess
import sys
import time
import psycopg2
from psycopg2.extras import RealDictCursor


# Initialisation
def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])


pkgs = ['flask', 'boto3', 'psycopg2']
for pkg in pkgs:
    install(pkg)

from flask import Flask
from flask import request
from flask import jsonify
import boto3
import json


def config():
    try:
        conn = psycopg2.connect(host="db.cyz8cc7o8ffv.us-east-1.rds.amazonaws.com",
                                port=5432,
                                database="sentiments",
                                user="postgres",
                                password="postgres")
    except:
        print("Unable to connect to the database")
    return conn


def get_res(keys, pattern):
    conn = config()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    inst = pattern
    sql = "select * " \
          "from %(inst)s1.sentiments as a, %(inst)s2.sentiments as b " \
          "where a.num  in %(vals)s or b.num in %(vals)s" % {
              "inst": inst, "vals": tuple(keys)}
    cursor.execute(sql)
    rows = cursor.fetchall()
    res = {}
    for row in rows:
        res[row['num']] = {
            'id': row['id'],
            'sentiment': row['sentiment'],
            'score': row['score']
        }
    print(res)
    return res


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
        # Remove non ascii characters from tweets
        value["text"] = value["text"].encode("ascii", "ignore").decode()
        tid = value['id']
        date = value['date']
        # Delete tweets without idea or date
        if tid is None or date is None:
            print("Deleted the tweet with id {} and date {}".format(tid, date))
            del data[k]
    return data


@app.route('/sentiment/proxy', methods=["POST", "GET"])
def proxy():
    if request.method != "POST":
        return "To access this service send a POST request to this URL with" \
               " the text you want analyzed in the body."
    else:
        data = request.get_json(force=True)
        # Saving the start time
        start_time = time.time()
        data = preprocess(data)
        to_send = {"pattern": "proxy",
                   "data": data}
        lh = call_lambda_handler1(to_send)
        res = {}
        res['lh1'] = lh['lh1']
        res['lh2'] = lh['lh2']
        res['time_to_predict_and_save_proxy'] = start_time - time.time()
        start_time = time.time()
        rows = get_res(data.keys(), "proxy")
        res['time_to_retrieve_proxy'] = start_time - time.time()
        print(json.dumps(res, indent=4, sort_keys=True))
        return jsonify(rows)


@app.route('/sentiment/sharding', methods=["POST", "GET"])
def sharding():
    if request.method != "POST":
        return "To access this service send a POST request to this URL with" \
               " the text you want analyzed in the body."
    else:
        data = request.get_json(force=True)
        # Saving the start time
        start_time = time.time()
        data = preprocess(data)
        to_send = {"pattern": "sharding",
                   "data": data}
        lh = call_lambda_handler1(to_send)
        res = {}
        res['lh1'] = lh['lh1']
        res['lh2'] = lh['lh2']
        res['time_to_predict_and_save_sharding'] = start_time - time.time()
        start_time = time.time()
        rows = get_res(data.keys(), "sharding")
        res['time_to_retrieve_sharding'] = start_time - time.time()
        print(json.dumps(res, indent=4, sort_keys=True))
        return jsonify(rows)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)

