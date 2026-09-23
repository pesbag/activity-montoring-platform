import io
import os
import json
import sys
import pandas as pd
from pathlib import Path
from confluent_kafka import Consumer, KafkaException, KafkaError
from clean_script import clean_script
bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

consumerConf = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'RawData',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumerConf)
running=True
measure_list=[]
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

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
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

    finally:
        consumer.close()

if __name__=="__main__":
    raw_producer_loop(consumer,["activity-readings"])