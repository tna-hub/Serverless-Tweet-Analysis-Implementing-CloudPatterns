import json
import io
import time

import psycopg2
from psycopg2.extensions import AsIs
import boto3
from random import choice

s3 = boto3.client('s3')
bucket: str = "aqui-tp3"  # Important!!! Set your bucket name here


# Connecting to the database
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


# This method uploads result to s3
def to_s3(data):
    data = json.dumps(data, indent=2).encode('utf-8')
    f = io.BytesIO(data)
    s3.upload_fileobj(f, bucket, "results.json")


def exec(conn, inst, k, t):  # Execute the insert to the database
    db = "%s.sentiments" % inst
    sql = "insert into %s (num, id, sentiment, score) VALUES %s"
    try:
        conn.cursor().execute(sql, (AsIs(db), (k, t['id'], t['sentiment'], t['score'])))
        conn.commit()
        return True, "Everything was inserted"
    except Exception as e:
        return False, str(e)


def proxy(conn, d):  # Proxy pattern
    for k, t in d.items():
        instance = choice(["proxy1", "proxy2"])  # Randomly selects the instance to save the data
        res, message = exec(conn, instance, k, t)
        if not res:
            break
    conn.cursor().close()
    conn.close()
    return message


def sharding(conn, d):  # sharding pattern
    n = choice(list(range(1, len(list(d)) + 1)))  # n first data goes to first instance
    i = 0
    for k, t in d.items():
        instance = "sharding1" if i < n else "sharding2"  # Specify the instance where the data will be saved
        res, message = exec(conn, instance, k, t)
        if not res:
            break
        i += 1
    conn.cursor().close()
    conn.close()
    return message


def lambda_handler(event, context):
    patt = event["pattern"]
    data = event["data"]
    to_s3(data)
    conn = config()
    start_time = time.time()  # Saving the start time
    if patt == "proxy":
        start_time = time.time()  # Saving the start time
        message = proxy(conn, data)
    elif patt == "sharding":
        start_time = time.time()  # Saving the start time
        message = sharding(conn, data)
    db_time = time.time() - start_time
    res = {"time": db_time,
           "message": message}
    return res
