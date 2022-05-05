#!/usr/bin/env python

"""Generates a stream to Kafka from a time series csv file.
"""

import argparse
import csv
import json
import sys
import time
from dateutil.parser import parse
from confluent_kafka import Producer
import socket
from pandas import read_csv


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg.value()), str(err)))
    else:
        print("Message produced: %s" % (str(msg.value())))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('filename', type=str,
                        help='Time series csv file.')
    parser.add_argument('topic', type=str,
                        help='Name of the Kafka topic to stream.')
    parser.add_argument('--speed', type=float, default=1, required=False,
                        help='Speed up time series by a given multiplicative factor.')
    args = parser.parse_args()

    topic = args.topic
    p_key = args.filename

    conf = {'bootstrap.servers': "localhost:9092",
            'client.id': socket.gethostname()}
    producer = Producer(conf)

    record_list = read_csv(args.filename).to_dict(orient='records')
    # i = 0

    for record in record_list:

        try:
            msg = json.dumps(record)
            producer.produce(topic, key=p_key, value=msg, callback=acked)
            # i += 1
            
            # if i % 12 == 0:
            #     time.sleep(2)


            producer.flush()

        except Exception as e:
            print("Error: %s" % (str(e)))
            sys.exit()


if __name__ == "__main__":
    main()
