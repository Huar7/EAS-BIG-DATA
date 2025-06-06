import pyspark as psp
from pyspark.sql import SparkSession
import psycopg2 as psy
from psycopg2 import OperationalError
import time


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
    cur.execute("select data from jajal")
    rows = cur.fetchall()
    for r in rows:
        print(f"hasil: {r}")

    con.close()


if __name__ == "__main__":
    main()
