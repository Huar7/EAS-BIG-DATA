import kafka as kf
import math
from yfinance import data
from send_db import send_val
import pandas as pd
from datetime import datetime
import pandas as pd
from pandas import DataFrame

# from multiprocessing import Process


def Rdata(iter, consumer, nil_one, spark, engine, data_last, unwanted_list):
    msg = consumer.poll(10)

    """
    Pyspark hanya menerima dalam bentuk Dictionaries, sehingga kita harus membuat fungsi yang mengubah setiap nilai yang ada di dalam fungsi tersebut menjadi suatu dict
    """

    nil_min_one = {}  # // nil_min_one atau nil[-1] adalah nilai yang akan menyimpan nilai
    if msg:
        for _, value in msg.items():
            try:
                nilai_panggilan = value[0].value
                nilai_data = nilai_panggilan[0]
                chronos = nilai_panggilan[1]
                # print(f"nilai Panggilan {nilai_panggilan} {'=' * 50}")

                if iter == 0:
                    print("langkah 1")
                    maxi = find_largest(nilai_data)
                    mixue = check_error(nilai_data)
                    maxum = dataframe_normalization(
                        mixue[0], len(mixue[0][maxi])
                    )  # // ini masih bisa mengalami kehancuran apabila ada yang nilainya 0
                    nil_min_one = maxum[-1]
                    print("langkah 2")
                    optimus_prime = god_merge(chronos, maxum)

                    df = spark.createDataFrame(
                        optimus_prime
                    )  # // NOTES tidak menemukan cara untuk membuat dataframe menjadi dalam bentuk / tidak perlu di sort column nya
                    df = df.toPandas()
                    print("panjang: ", len(df))

                    send_val(df, engine)

                    print("langkah 2.0.0")
                    return (0, nil_min_one, df, mixue[1])
                else:
                    print("langkah 3")
                    if unwanted_list:
                        print(unwanted_list)
                        print("gak kosong")
                        for j in unwanted_list:
                            print(j)
                            for xn in j:
                                del nilai_data[xn]
                    maxum = datastream_normalization(
                        nilai_data, chronos, nil_one
                    )  # // nil min one kosong
                    print("langkah 4")
                    stream_frame = spark.createDataFrame(maxum)
                    print("langkah 5")
                    df = stream_frame.toPandas()
                    print("ok 1")

                    """pada saat ini kita memiliki 2 data dimana satu mengandung nilai terdahulu sedangkan satu lagi mengandung nilai terbaru, maka dari itu kita akan menggabungkan kedua nilai itu dengan menggunakan fungsi union yang dimiliki class DataFrame"""

                    df = pd.concat([df, data_last])
                    print("panjang: ", len(df))
                    send_val(df, engine)

                    return (0, nil_one, df, unwanted_list)

                    # -- NOTES;

            except:
                print("Kode I, Lompati atau Tidak Bisa")
                return (1, nil_min_one, None, [])
    else:
        print("No new messages \n")
        return (1, {}, None, [])


def find_largest(nil: dict) -> str:
    panjang = []
    naila = [*nil]
    for i in naila:
        panjang.append(len(nil[i]))
    maxi = max(panjang)
    posisi = panjang.index(maxi)
    pos = naila[posisi]
    return pos


def check_error(data_input: dict):
    """Fungsi ini digunakan untuk mengantisipasi apabila salah satu ticker tidak memiliki satupun nilai"""
    hasil = []
    for i in [*data_input]:
        if len(data_input[i]) == 0:
            print("i sama dengan: ", i)
            hasil.append([i])
            del data_input[i]
    return (data_input, hasil)


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
