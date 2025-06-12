from numpy import append, save
from requests import options
from pyspark.sql import SparkSession, DataFrameWriterV2
import psycopg2 as psy

import pandas as pd

import yfinance

from sqlalchemy import create_engine


def main():
    con = psy.connect(
        host="localhost",
        database="NurHary",
        user="NurHary",
        password="ForourDreams",
        port="5431",
    )
    print("connect?")
    cur = con.cursor()
    cur.execute("select istri from jajal")
    rows = cur.fetchall()
    for r in rows:
        print(f"hasil: {r}")

    con.close()


def main2():
    jajal = [
        {"ageh": 20, "istri": 4, "bcount": 30},
        {"ageh": 31, "istri": 200, "bcount": 300000},
        {"ageh": 67, "istri": 0, "bcount": 2},
        {"ageh": 10, "istri": 6106, "bcount": 125021493104821051623},
        {"ageh": 25, "istri": 136013, "bcount": 1},
        {"ageh": 28, "istri": 1, "bcount": 1000},
        {"ageh": 4, "istri": 0, "bcount": 1},
        {"ageh": 35, "istri": 1, "bcount": 34},
    ]
    spark = SparkSession.builder.appName("main_app").getOrCreate()
    absen = spark.createDataFrame(jajal)
    absenpdf = absen.toPandas()
    print(absen)
    engine = create_engine("postgresql://NurHary:ForourDreams@localhost:5431/NurHary")
    print(type(engine))

    iter = 0
    while True:
        usbun = pd.DataFrame(
            {"ageh": [6969, 2020], "istri": [24, 100], "bcount": [124015, 0]}
        )
        if iter == 0:
            send_val(absenpdf, engine)
        else:
            absenpdf = pd.concat([absenpdf, usbun])
            send_val(absenpdf, engine)
        if iter == 5:
            break
        iter += 1


def send_val(data, engine):
    con = psy.connect(
        host="localhost",
        database="NurHary",
        user="NurHary",
        password="ForourDreams",
        port="5431",
    )
    cur = con.cursor()
    cur.execute("drop table if exists jajal2;")
    con.commit()

    data.to_sql("jajal2", engine, index=False)
    cur.close()
    con.close()


def main3():
    yfinance.Tickers(["CHYM"])
    yfinance.history()


if __name__ == "__main__":
    main2()
