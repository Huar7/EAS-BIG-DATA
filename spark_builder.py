import pyspark
from pyspark.sql import SparkSession

import kafka as kf

import json
import math

from yfinance import data

# from multiprocessing import Process


def Rdata(iter):
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

    """
    Pyspark hanya menerima dalam bentuk Dictionaries, sehingga kita harus membuat fungsi yang mengubah setiap nilai yang ada di dalam fungsi tersebut menjadi suatu dict
    """

    nil_min_one = {}  # // nil_min_one atau nil[-1] adalah nilai yang akan menyimpan nilai

    if iter != 0:
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
                        nil_min_one = maxum[-1]
                        optimus_prime = god_merge(chronos, maxum)

                        df = spark.createDataFrame(
                            optimus_prime
                        )  # // NOTES tidak menemukan cara untuk membuat dataframe menjadi dalam bentuk / tidak perlu di sort column nya
                        df.show()
                        once += 1
                    else:
                        maxum = datastream_normalization(
                            nilai_data, chronos, nil_min_one
                        )
                        stream_frame = spark.createDataFrame(maxum)
                        stream_frame.show()
                        print("ok 1")

                        """pada saat ini kita memiliki 2 data dimana satu mengandung nilai terdahulu sedangkan satu lagi mengandung nilai terbaru, maka dari itu kita akan menggabungkan kedua nilai itu dengan menggunakan fungsi union yang dimiliki class DataFrame"""
                        df = df.union(
                            stream_frame
                        )  # // tidak usah pedulikan eror, karena run pertama pasti terjadi
                        print(df.tail(1))

                    # -- NOTES;

                except:
                    print("Kode I, Lompati atau Tidak Bisa")
        else:
            print("No new messages \n")


def find_largest(nil: dict) -> str:
    panjang = []
    naila = [*nil]
    for i in naila:
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


def datastream_normalization(data: dict, data2: dict, andval: dict):
    """Fungsi yang digunakan untuk mengonversikan data input menjadi data yang siap digunakan untuk dibuat dataframe dan untuk mengisi nilai nan dengan nilai terdahulu"""
    junia = [*data]  # // mendapatkan list keys dari dict data
    for i in junia:
        if math.isnan(data[i]):  # // pengecekan apakah nilai tidak nan
            data[i] = andval[i]  # // konversi nilai kosong menjadi nilai terakhir
    data3 = []
    data3.append(data2 | data)
    print("data33", data3)
    return data3


def god_merge(data1, data2):
    """Fungsi yang digunakan untuk menggabungkan 2 list dengan isi Dictionaries dengan update()"""
    data3 = []
    for i in range(len(data1)):
        data3.append(data1[i] | data2[i])
    return data3


if __name__ == "__main__":
    Rdata()
