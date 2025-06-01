import kafka as kf
from kafka.admin import KafkaAdminClient, NewTopic

import json

# from multiprocessing import Process
import time

import requests
from bs4 import BeautifulSoup

import yfinance as yf
import pandas as pd


def yf_first(isi: list, waktu):
    starter = yf.download(isi, period="5y", interval="1wk")
    hasil = {}
    once = 0
    nulia = []
    for i in starter["Close"]:
        julia = starter["Close"][i].dropna()
        nalia = []
        for kukukaka in range(len(julia)):
            nalia.append(julia[kukukaka].item())
            if once == 0:
                nulia.append(
                    {"Timestamp": julia.index[kukukaka].strftime("%Y/%m/%d %X")}
                )
        once = 1

        hasil[i] = nalia
    return (hasil, nulia)


def data_ingest_run(isi: list, waktu):
    info = yf.Tickers(isi)
    hasil = {}
    try:
        historis = info.history(period="1d", interval="1m", progress=False, repair=True)
        reynauld = {"Timestamp": historis.index[-1].strftime("%Y/%m/%d %X")}
        for i in historis["Close"]:
            dismas = historis["Close"][i].iloc[-1]
            hasil[i] = dismas  # // perubahan pada output
        return (hasil, reynauld)
    except:
        print("mencoba mengulang server")
        data_ingest_run(isi)


if __name__ == "__main__":
    indes = ["BTC-USD", "XRP-USD", "NVAX"]
    data_ingest_run(indes)


def parse(source):
    soup = BeautifulSoup(source.content, "html.parser")

    ticker = []
    print(soup)
    for tbody in soup.find_all("tbody"):
        print("sukses 1")
        for tr in tbody.findAll("tr"):
            print("sukses 2")
            for tickerCol in tr.findAll("td"):
                # print(tickerCol)
                for span in tickerCol.findAll("span"):
                    print("Mie Sukses isi 2")
                    txt = span.get_text()
                    # Encoding in utf-8 to remove the u character from the list
                    ticker.append(txt.encode("utf-8"))
                    break
                break  # .. ini untuk melompati nilai bawahnya

    ticker = [t.decode("utf-8").strip() for t in ticker]
    return ticker


def trending():
    session = requests.Session()
    url = "https://finance.yahoo.com/trending-tickers"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"  # // emulasikan model browser
    }
    req = session.get(url, headers=headers)
    trend = parse(req)
    return trend


def Wdata(waktu, iter):
    indes = trending()
    print(
        "menyiapkan mesin"
    )  # // memberikan waktu untuk kafka untuk berjalan terlebih dahulu
    time.sleep(30)  # // untuk memastikan apache kafka telah berjalan
    once = 0
    start_val = yf_first(indes)
    while True:
        continous_val = data_ingest_run(indes)

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
