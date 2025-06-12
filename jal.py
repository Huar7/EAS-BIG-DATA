from numpy import save
from requests import options
from pyspark.sql import SparkSession, DataFrameWriterV2
import psycopg2 as psy


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
    spark = (
        SparkSession.builder.appName("main_app")
        .config("spark.jars", "jars/postgresql-42.2.27.jar")
        .getOrCreate()
    )
    absen = spark.createDataFrame(jajal)
    absen.show()
    jdbc_url = "jdbc:postgresql://localhost:5431/NurHary"
    absen.write.jdbc(url=jdbc_url, table="jajal", mode="append", properties={"user":"NurHary","password":"ForourDreams", "driver":"org.postgresql.Driver"})


if __name__ == "__main__":
    main()
