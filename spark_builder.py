import pyspark as psp
from pyspark.sql import SparkSession

import kafka as kf
import ast

import json
import main
from multiprocessing import Process



def Rdata(): 
    
    consumer = kf.KafkaConsumer('stock_kotor',
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=False,
                         group_id='my_group_id',
                         value_deserializer=lambda x: x.decode('utf8')
                        )
    consumer.subscribe(topics=['stock_kotor'])
    once = 0
    while True:
        msg = consumer.poll(timeout_ms=1000)
        if msg:
            for key, value in msg.items():
                print("ada nilai kafka masuk berupa: Kunci: {} | Nilainya_brot: {}".format(key, value[0]))
                try:
                    print("\n", 50*"=", "\n", ast.literal_eval(value[0].value))
                    try:
                        # // FixMe: ini kenapa tetap str terus ya su
                        nilai_panggilan = ast.literal_eval(value[0].value)

                        print(type(nilai_panggilan))
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

if __name__ == "__main__":
    Rdata()
