import time
import yfinance as yf
import pandas as pd


def yf_first(isi: list):
    starter = yf.download(isi,period="5y", interval="1wk") 
    hasil = {}
    for i in starter['Close']:
        julia = starter['Close'][i].dropna()
        nilui = {}
        for kukukaka in range(len(julia)):
            nilui[julia.index[kukukaka].strftime("%Y/%m/%d")] = julia[kukukaka].item()
            hasil[i] = nilui
    return(hasil)

def data_ingest_run(isi: list):
    info = yf.Tickers(isi)
    hasil = {}
    try:
        historis = info.history(period='1d', interval='1m', progress=False, repair=True)
        reynauld = historis.index[-1].strftime("%Y/%m/%d")
        time_point = ""
        if reynauld != time_point:
            for i in historis['Close']:
                if pd.isna(historis["Close"][i].iloc[-1]):
                    print("Melompati Karena Kosong", i)
                else:
                    dismas = historis['Close'][i].iloc[-1]
                    hasil[i] = {reynauld: dismas.item()}
            time_point = reynauld
        else:
            print("Melompati, Dikarenakan tidak ada update")
    except:
        print(f"mencoba mengulang server")
        data_ingest_run(isi)
    return(hasil)

if __name__ == '__main__':
    data_ingest_run()
