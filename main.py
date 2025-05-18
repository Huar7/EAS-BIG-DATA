import kafka as kf
from kafka.admin import KafkaAdminClient, NewTopic
import pyspark
from json import dump
import yfinance as yf
import asyncio


# // membuat admin untuk membuat topic kafka
sosok_admin = KafkaAdminClient(bootstrap_servers=['localhost:9092'], client_id='client_1')
gosip = NewTopic(name="utama", num_partitions=1, replication_factor=1)
sosok_admin.create_topics(new_topics=[gosip], validate_only=False)


consumer = kf.KafkaConsumer('main',
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=False,
                         group_id='my_group_id',
                         value_deserializer=lambda x: x.decode('utf-8')
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
