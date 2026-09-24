import datetime
import io
import os
import json
import sys
import socket
import time
from collections import deque
from confluent_kafka import Producer
import pandas as pd
from pathlib import Path
from confluent_kafka import Consumer, KafkaException, KafkaError
from clean_script import clean_script
bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

producerConf = {
    'bootstrap.servers': bootstrap_servers,
    'client.id': socket.gethostname()
}

consumerConf = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'RawData',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumerConf)
producer= Producer(producerConf)

running=True
def raw_producer_loop(consumer,topics):
    try:
        consumer.subscribe(topics)
        counter=0
        while running:
            msg=consumer.poll(timeout=2)
            if msg is None:
                if counter == 0:
                    continue
                else:
                    print(f"no more message recived.total messages: {counter}")
                    break

            # if msg.error():
            #     if msg.error().code() == KafkaError._PARTITION_EOF:
            #         sys.stderr.write('%% %s %d reached end at offset %d\n' %
            #                          (msg.topic(), msg.partition(), msg.offset()))
            #         continue
            #     elif msg.error().code() in (KafkaError.UNKNOWN_TOPIC_OR_PART, 3):
            #         time.sleep(0.5)
            #         continue
            #     else:
            #         raise KafkaException(msg.error())
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write('%% %s %d reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                counter+=1
                raw_text = msg.value().decode("utf-8")
                data_dict=json.loads(raw_text)
                df=pd.DataFrame([data_dict])
                clean_df=clean_script(df)
                if len(clean_df) == 0:
                    continue
                dict_record=clean_df.to_dict(orient='records')[0]
                print(f"{dict_record} total {counter}")
                calc_output=hande_measure(dict_record)
                if calc_output is not None:
                    if calc_output["severity"]!="Normal":
                        clean_json = json.dumps(calc_output,default=str).encode('utf-8')
                        producer.produce(topic='anomalies',
                                         value=clean_json
                                         )
                        producer.poll(0)
    finally:
        print("flushing remaining messages and closing consumer...")
        producer.flush()
        consumer.close()

WINDOW_SIZE = int(os.getenv("WINDOW_SIZE", 20))
Z_THRESHOLD=float(os.getenv("Z_THRESHOLD",3.5))
CRITICAL_THRESHOLD=float(os.getenv("CRITICAL_THRESHOLD",4.5))
station_states={}

def hande_measure(incoming_reading):
    source_id=incoming_reading["source_id"]
    if source_id not in station_states:
        station_states[source_id]=deque(maxlen=WINDOW_SIZE)
    window = station_states[source_id]
    if len(window)<WINDOW_SIZE:
        window.append(incoming_reading)
        return

    values_series = pd.Series([item["value"] for item in window])

    mean = values_series.mean()
    std = values_series.std(ddof=1)

    if std == 0 or pd.isna(std):
        window.append(incoming_reading)
        return

    z_score = (incoming_reading["value"] - mean) / std

    if abs(z_score) >= CRITICAL_THRESHOLD:
        incoming_reading["severity"] = "Critical"
    elif abs(z_score) >= Z_THRESHOLD:
        incoming_reading["severity"]="Warning"
    elif abs(z_score) < Z_THRESHOLD:
        incoming_reading["severity"]="Normal"

    incoming_reading["mean"]=mean
    incoming_reading["standard_deviation"] = std
    incoming_reading["z_score"] = z_score
    incoming_reading["detected_at"] = datetime.datetime.now()
    incoming_reading["status"]="New"

    return  incoming_reading

if __name__=="__main__":
    raw_producer_loop(consumer,["activity-readings"])