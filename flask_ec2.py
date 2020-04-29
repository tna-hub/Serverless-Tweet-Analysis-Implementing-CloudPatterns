from __future__ import print_function
import subprocess
import sys
import time


# Initialisation
def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])


pkgs = ['flask', 'boto3', 'psycopg2-binary']
for pkg in pkgs:
    install(pkg)

from flask import Flask
from flask import request
from flask import jsonify
import boto3
import json
import psycopg2
from psycopg2.extras import RealDictCursor


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
    sql = "select * " \
          "from %(pattern)s1.sentiments " \
          "where num  in %(vals)s "\
          "UNION " \
          "select * " \
          "from %(pattern)s2.sentiments " \
          "where num  in %(vals)s" % {
              "pattern": pattern, "vals": tuple(keys)}
    cursor.execute(sql)
    rows = cursor.fetchall()
    res = {}
    for row in rows:
        res[row['num']] = {
            'id': row['id'],
            'sentiment': row['sentiment'],
            'score': row['score']
        }
    conn.close()
    return res

#Declaring the flask app
app = Flask(__name__)

# This method is calling another Lambda function
def call_lambda_handler1(data):
    client = boto3.client("lambda")
    response = client.invoke(
        FunctionName='lambda_handler1',
        InvocationType='RequestResponse',
        LogType='None',
        Payload=json.dumps(data),
    )
    return json.load(response['Payload'])


#This method preprocesses the tweets (removing non ascii characteers)
def preprocess(data):
    for k, value in list(data.items()):
        # Remove non ascii characters from tweets
        value["text"] = value["text"].encode("ascii", "ignore").decode()
        tid = value['id']
        date = value['date']
        # Delete tweets without idea or date
        if tid is None or date is None:
            print("Deleted a tweet with no id or date")
            del data[k]
    return data


# Route for proxy pattern
@app.route('/sentiment/proxy', methods=["POST", "GET"])
def proxy():
    if request.method != "POST":
        return "To access this service send a POST request to this URL with" \
               " the text you want analyzed in the body."
    else:
        pattern = "proxy"
        start_time_save = time.time()  # Saving the start time to process and save
        data = request.get_json(force=True)
        data = preprocess(data)
        to_send = {"pattern": pattern,
                   "data": data}
        lh = call_lambda_handler1(to_send)
        res = {'lh1': lh['lh1'], 'lh2': lh['lh2'],
               'time_to_predict_and_save_{}'.format(pattern): time.time() - start_time_save}
        start_time_retrieve = time.time()  # Saving the start time to retrieve
        keys = [int(i) for i in data.keys()]
        rows = get_res(keys, pattern)
        res['time_to_retrieve_{}'.format(pattern)] = time.time() - start_time_retrieve
        res['whole_time'] = time.time() - start_time_save
        print(json.dumps(res, indent=4, sort_keys=True))
        return jsonify(rows)


# Route for sharding pattern
@app.route('/sentiment/sharding', methods=["POST", "GET"])
def sharding():
    if request.method != "POST":
        return "To access this service send a POST request to this URL with" \
               " the text you want analyzed in the body."
    else:
        pattern = "sharding"
        start_time_save = time.time()  # Saving the start time to process and save
        data = request.get_json(force=True)
        data = preprocess(data)
        to_send = {"pattern": pattern,
                   "data": data}
        lh = call_lambda_handler1(to_send)
        res = {'lh1': lh['lh1'], 'lh2': lh['lh2'], 'time_to_predict_and_save_{}'.format(pattern): time.time() - start_time_save}
        start_time_retrieve = time.time()  # Saving the start time to retrieve
        keys = [int(i) for i in data.keys()]
        rows = get_res(keys, pattern)
        res['time_to_retrieve_{}'.format(pattern)] = time.time() - start_time_retrieve
        res['whole_time'] = time.time() - start_time_save
        print(json.dumps(res, indent=4, sort_keys=True))
        return jsonify(rows)


# Launching the app
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)

