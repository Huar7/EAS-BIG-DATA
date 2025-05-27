import kafka as kf
from kafka.admin import KafkaAdminClient, NewTopic
import pyspark as psp
from pyspark.sql import SparkSession

import json
from multiprocessing import Process
import main2
import time

def belajar():
# // membuat admin untuk membuat topic kafka
    sosok_admin = KafkaAdminClient(bootstrap_servers=['localhost:9092'], client_id='client_1')
    gosip = NewTopic(name="utama", num_partitions=1, replication_factor=1)
    sosok_admin.create_topics(new_topics=[gosip], validate_only=False)


    consumer = kf.KafkaConsumer('main',
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=False,
                         group_id='my_group_id',
                         value_deserializer=lambda x: x.decode('utf8')
                        )
    consumer.subscribe(topics=['main'])

# // menunggu masukan baru
    while True:
        msg = consumer.poll(timeout_ms=1000)
        if msg:
            for key, value in msg.items():
                print("Key: {} | Value: {}".format(key, value))
        else:
            print("No new messages")

            


def Wdata():
    once = 0
    indes = ["BTC-USD", "XRP-USD", "NVAX", "MSFT", "AAPL"]
    start_val = main2.yf_first(indes)
    while True:

        continous_val = main2.data_ingest_run(indes)

        print(f"start value: {type(start_val)}, continous value: {type(continous_val)}")

        produser = kf.KafkaProducer(bootstrap_servers='localhost:9092')
        if once == 0:
            print(" \n kirim 1")
            produser.send(topic='stock_kotor', value= json.dumps(start_val, default=str).encode('utf-8'))
            once += 1
            print(" \n sukses 1")
        else:
            print(" \n kirim 2")
            produser.send(topic='stock_kotor', value= json.dumps(continous_val, default=str).encode('utf-8'))
            print(" \n sukses 2")
            # break
        produser.flush()
        time.sleep(10)
            





if __name__ == '__main__':
    Wdata()
