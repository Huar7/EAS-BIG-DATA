import pyspark as psp
from pyspark.sql import SparkSession

import kafka as kf
import ast

import json

# from multiprocessing import Process

def Rdata(): 
    spark = SparkSession.builder.appName('main_app').getOrCreate()
    consumer = kf.KafkaConsumer('stock_kotor',
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=False,
                         group_id='my_group_id',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                        )
    consumer.subscribe(topics=['stock_kotor'])
    once = 0
    while True:
        msg = consumer.poll(timeout_ms=1000)
        if msg:
            for key, value in msg.items():
                print("ada nilai kafka masuk berupa")
                try:
                    print("\n", 50*"=", "\n", value[0].value)
                    try:
                        # // FixMe: ini kenapa tetap str terus ya: ubah semua Timestamp menjadi dalam bentuk str
                        nilai_panggilan = value[0].value
                        output = {}
                        for i in [*nilai_panggilan]:
                            output[i] = spark.createDataFrame(nilai_panggilan[i])

                    except:
                        print("Kode II, Tidak Bisa")
                except:
                    print("Kode I, Lompati atau Tidak Bisa")
        else:
            print("No new messages \n")
        if once <= 55:
            once += 1
        else:
            break
    for i in [*output]:
        output[i].show()

if __name__ == "__main__":
    Rdata()
