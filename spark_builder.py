from kafka.protocol.types import Schema
import pyspark
from pyspark.sql import SparkSession

import kafka as kf

import json

from yfinance import data

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
    column = pyspark.sql.types.StructType([])

    """
    Pyspark hanya menerima dalam bentuk Dictionaries, sehingga kita harus membuat fungsi yang mengubah setiap nilai yang ada di dalam fungsi tersebut menjadi suatu dict
    """

    while True:
        msg = consumer.poll(timeout_ms=1000)
        if msg:
            for _, value in msg.items():
                try:
                    # // FixMe: ini kenapa tetap str terus ya: ubah semua Timestamp menjadi dalam bentuk str
                    nilai_panggilan = value[0].value
                    nilai_data = nilai_panggilan[0]
                    chronos = nilai_panggilan[1]
                    print(f"nilai Panggilan {nilai_panggilan} {'=' * 50}")

                    if (
                        once == 0
                    ):  # // ini untuk mengatasi ketimpangan data awalan (Downloads)
                        maxi = find_largest(nilai_data)
                        print(maxi)
                        maxum = dataframe_normalization(
                            nilai_data, len(nilai_data[maxi])
                        )
                        optimus_prime = god_merge(chronos, maxum)

                        df = spark.createDataFrame(data=optimus_prime)
                        df.show()
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


def dataframe_normalization(data: dict, amount: int):
    """Fungsi yang digunakan untuk mengubah bentuk data list dengan dictionaries yang berisi list untuk berubah menjadi , Amount adalah jumlah seharusnya dari data tersebut"""
    junia = [*data]
    for i in junia:
        iterasi = amount - len(data[i])
        match iterasi:
            case 0:
                pass
            case _:
                for _ in range(iterasi):
                    data[i].append(
                        data[i][-1]
                    )  # // ini untuk menormalisasi data yang kosong dimana akan mengisi dengan nilai yang atas
    to_dict = [dict(zip(data.keys(), values)) for values in zip(*data.values())]

    return to_dict


def god_merge(data1, data2):
    """Fungsi yang digunakan untuk menggabungkan 2 list dengan isi Dictionaries dengan update()"""
    data3 = []
    for i in range(len(data1)):
        data3.append(data1[i] | data2[i])
    print(data3)
    return data3


if __name__ == "__main__":
    Rdata()
