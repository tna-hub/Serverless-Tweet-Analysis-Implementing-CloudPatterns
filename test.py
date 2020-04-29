import subprocess
import sys

from flask_ec2 import config


def delete():
    sql = 'TRUNCATE proxy1.sentiments;\
        TRUNCATE proxy2.sentiments;\
        TRUNCATE sharding1.sentiments;\
        TRUNCATE sharding2.sentiments;'

    conn = config()
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()


def start(pattern):
    """subprocess.call(
        ["curl", "-X", "POST",
         "Content-type: application/json", "--data", "@/home/aqui/data.json",
         "192.168.13.138:5000/sentiment/{}".format(pattern)])"""
    process = subprocess.Popen('curl -X POST -H'
                               '"Content-type: application/json" --data @/home/aqui/data.json '
                               '"192.168.13.138:5000/sentiment/{}"'.format(pattern),
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               shell=True)
    output = process.stdout.read()
    print(output.decode())

delete()
for i in range(0, 10):
    start("proxy")
    delete()
