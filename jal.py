from datetime import datetime
from numpy import append, save
from requests import options
from pandas._libs.tslibs.period import period_array_strftime
from pyspark.sql import SparkSession, DataFrameWriterV2
import psycopg2 as psy

import pandas as pd

from statsmodels.tsa.arima.model import ARIMA
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

    print(tuple(row for row in absenpdf))

    # usbun = pd.DataFrame()
    # engine = create_engine("postgresql://NurHary:ForourDreams@localhost:5431/NurHary")
    # absenpdf.to_sql("jajal2", engine)
    # print(type(absenpdf))

    # absenpdf.append({"ageh": 69, "istri": 69, "bcount": 69})

    cur.close()
    con.close()


def main3():
    starter = yfinance.Tickers(["AAL", "AAOI", "BA"])
    historis = starter.history(period="1d", interval="1m", progress=False, repair=True)



def main4():
    sekarang = datetime.now()
    print(type(sekarang))



if __name__ == "__main__":
    main2()
