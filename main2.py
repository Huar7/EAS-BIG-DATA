import yfinance as yf
import pandas as pd


def yf_first(isi: list):
    starter = yf.download(isi,period="5y", interval="1wk") 
    hasil = {}
    once = 0
    nulia = []
    for i in starter['Close']:
        julia = starter['Close'][i].dropna()
        nalia = []
        for kukukaka in range(len(julia)):
            nalia.append(julia[kukukaka].item())
            if once == 0:
                nulia.append({"Timestamp":julia.index[kukukaka].strftime("%Y/%m/%d %X")})
        once = 1

        hasil[i] = nalia 
    return(hasil, nulia)

def data_ingest_run(isi: list):
    info = yf.Tickers(isi)
    hasil = {}
    try:
        historis = info.history(period='1d', interval='1m', progress=False, repair=True)
        reynauld = {"Timestamp" : historis.index[-1].strftime("%Y/%m/%d %X")}
        for i in historis['Close']:
            dismas = historis['Close'][i].iloc[-1]
            hasil[i] = (dismas) # // perubahan pada output
        return(hasil, reynauld)
    except:
        print("mencoba mengulang server")
        data_ingest_run(isi)

if __name__ == '__main__':
    indes = ["BTC-USD", "XRP-USD", "NVAX"]
    data_ingest_run(indes)
