import pyspark as psp
from pyspark.sql import SparkSession

import kafka as kf
import ast

import json

# from multiprocessing import Process


def Rdata():
    spark = SparkSession.builder.appName("main_app").getOrCreate()
    consumer = kf.KafkaConsumer(
        "stock_kotor",
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="latest",  # // ini untuk model pengambilan data: latest untuk mengambil data yang baru di luncurkan
        enable_auto_commit=False,  # //
        group_id="my_group_id",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    consumer.subscribe(topics=["stock_kotor"])
    once = 0

    while True:
        msg = consumer.poll(timeout_ms=1000)
        if msg:
            for _, value in msg.items():
                try:
                    # // FixMe: ini kenapa tetap str terus ya: ubah semua Timestamp menjadi dalam bentuk str
                    nilai_panggilan = value[0].value
                    print(f"nilai Panggilan {nilai_panggilan} {'=' * 50}")

                    if (
                        once == 0
                    ):  # // ini untuk mengatasi ketimpangan data awalan (Downloads)
                        maxi = find_largest(nilai_panggilan)
                        print(maxi)
                    # -- NOTES;

                    once += 1
                    # hasil_output = {}
                    # for i in [*nilai_panggilan]:
                    #     hasil_output[i] = nilai_panggilan[i]

                except:
                    print("Kode I, Lompati atau Tidak Bisa")
        else:
            print("No new messages \n")
        if once >= 5:
            break  # // comment ini untuk membiarkan loop tanpa batas


def find_largest(nil: dict) -> str:
    panjang = []
    naila = [*nil]
    for i in naila:
        print(f"panjang dari {i} adalah: {len(nil[i])}")
        panjang.append(len(nil[i]))
    maxi = max(panjang)
    posisi = panjang.index(maxi)
    pos = naila[posisi]
    return pos


if __name__ == "__main__":
    Rdata()
