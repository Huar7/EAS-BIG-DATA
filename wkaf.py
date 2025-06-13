import kafka as kf
from kafka.admin import KafkaAdminClient, NewTopic

import json

# from multiprocessing import Process
import time

import requests
from bs4 import BeautifulSoup

from pandas.core.dtypes.inference import iterable_not_string
import yfinance as yf
import pandas as pd

import spark_builder as sb


# ini untuk melakukan Scrapping dan mendapatkan stock yang paling populer
# scrapping ini menggunakan Beautifullsoup
def parse(source):
    soup = BeautifulSoup(source.content, "html.parser")

    ticker = []
    for tbody in soup.find_all("tbody"):
        for tr in tbody.findAll("tr"):
            for tickerCol in tr.findAll("td"):
                # print(tickerCol)
                for span in tickerCol.findAll("span"):
                    txt = span.get_text()
                    # Encoding in utf-8 to remove the u character from the list
                    ticker.append(txt.encode("utf-8"))
                    break
                break  # .. ini untuk melompati nilai bawahnya

    ticker = [t.decode("utf-8").strip() for t in ticker]
    return ticker


def trending():
    session = requests.Session()
    url = "https://finance.yahoo.com/markets/stocks/trending/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"  # // emulasikan model browser
    }
    req = session.get(url, headers=headers)
    trend = parse(req)
    return trend


def yf_first(isi: list, waktu: str):
    starter = yf.Tickers(isi)
    historis = starter.history(period=waktu, interval="1h", progress=False, repair=True)

    # kembalikan = ['Dividends', 'High', 'Low', 'Open', 'Stock Splits', 'Volume'] # // ini adalah daftar variabel yang nilainya akan di return, tidak secara programming untuk optimisasi

    # // apakah benar - benar dibutuhkan variabel lainnya?

    hasil = {}  # // ini untuk mengatasi nilai Close
    once = 0
    nulia = []  # // ini untuk mengatasi nilai Timestamp
    for i in historis["Close"]:
        julia = historis["Close"][
            i
        ].dropna()  # // kita harus mengembalikan nilai selain close juga D;
        nalia = []
        for j in range(len(julia)):
            nalia.append(julia[j].item())  # // return data
            if once == 0:
                nulia.append({"Timestamp": julia.index[j]})
        once = 1

        hasil[i] = nalia
    # for i in historis:

    return (hasil, nulia)


def data_ingest_run(isi: list):
    info = yf.Tickers(isi)
    hasil = {}
    try:
        historis = info.history(period="1d", interval="1h", progress=False, repair=True)
        reynauld = {"Timestamp": historis.index[-1]}
        for i in historis["Close"]:
            dismas = historis["Close"][i].iloc[-1]
            hasil[i] = dismas  # // perubahan pada output
        return (hasil, reynauld)
    except:
        print("mencoba mengulang server")
        data_ingest_run(isi)


# ini untuk menerima nilai dari yfinance tersebut dan langsung mengirimnya ke kafka


def Wdata(waktu: str, iter, indes):
    produser = kf.KafkaProducer(bootstrap_servers="localhost:9092")
    if iter == 0:
        start_val = yf_first(indes, waktu)

        produser.send(
            topic="stock_kotor",
            value=json.dumps(start_val, default=str).encode("utf-8"),
        )
        produser.flush()

    else:
        print("jal1")
        continous_val = data_ingest_run(indes)
        produser.send(
            topic="stock_kotor",
            value=json.dumps(continous_val, default=str).encode("utf-8"),
        )
        produser.flush()


if __name__ == "__main__":
    Wdata("1wk", 0)
