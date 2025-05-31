import kafka as kf
from kafka.admin import KafkaAdminClient, NewTopic
# import pyspark as psp
# from pyspark.sql import SparkSession

import json
import get_trending

# from multiprocessing import Process
import main2
import time


def Wdata():
    indes = get_trending.trending()
    print(
        "menyiapkan mesin"
    )  # // memberikan waktu untuk kafka untuk berjalan terlebih dahulu
    time.sleep(30)  # // untuk memastikan apache kafka telah berjalan
    once = 0
    start_val = main2.yf_first(indes)
    while True:
        continous_val = main2.data_ingest_run(indes)

        print(f"start value: {type(start_val)}, continous value: {type(continous_val)}")

        produser = kf.KafkaProducer(bootstrap_servers="localhost:9092")
        if once == 0:
            print(" \n kirim 1")
            produser.send(
                topic="stock_kotor",
                value=json.dumps(start_val, default=str).encode("utf-8"),
            )
            once += 1
            print(" \n sukses 1")
        else:
            print(" \n kirim 2")
            produser.send(
                topic="stock_kotor",
                value=json.dumps(continous_val, default=str).encode("utf-8"),
            )
            print(" \n sukses 2")
            # break
        produser.flush()
        time.sleep(10)


if __name__ == "__main__":
    Wdata()
