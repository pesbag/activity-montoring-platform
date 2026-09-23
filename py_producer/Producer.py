import csv
import json
import os
import time
import pandas as pd
from confluent_kafka import Producer
import socket
from pathlib import Path

bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

conf = {
    'bootstrap.servers': bootstrap_servers,
    'client.id': socket.gethostname()
}
producer = Producer(conf)

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

def main():
    print("enter to main")
    BASE_DIR=Path(__file__).resolve().parent
    file_path = BASE_DIR / "Data" / "activity_readings.csv"
    try:
        df=pd.read_csv(file_path,
                       dtype={
                           "event_id":"str",
                            "source_id":"str",
                            "value":"str",
                       },
                        parse_dates=["timestamp"]
                    )
        DELAY_TIME= float(os.getenv("KAFKA_EVENT_DELAY_SECONDS",0.2))
        sort_by_date=df.sort_values(by= "timestamp",ascending=True)
        sort_by_date["timestamp"]=sort_by_date["timestamp"].astype(str)
        for row in sort_by_date.to_dict(orient="records"):
            producer.produce(topic='activity-readings',
                             value=json.dumps(row).encode('utf-8'),
                             callback=acked
                             )
            time.sleep(DELAY_TIME)
            producer.poll(0)
        print("flushing remaining messages...")
        producer.flush()
        print("all messages delivered")
    except FileNotFoundError:
        print(f"error file {file_path} not found")

if __name__=="__main__":
    main()