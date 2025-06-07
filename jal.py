import json

import psycopg2 as psy

import kafka as kf


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


def main2():
    consumer = kf.KafkaConsumer(
        "stock_kotor",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="latest",  # // ini untuk model pengambilan data: latest untuk mengambil data yang baru di luncurkan
        enable_auto_commit=False,  # //
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    consumer.subscribe(["stock_kotor"])
    
    msg = consumer.poll(timeout_ms=1000) # // ini biang keroknya suuu
    if msg:
        print(msg)
        return 0
    main2()


if __name__ == "__main__":
    main2()
