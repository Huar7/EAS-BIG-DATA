import yfinance as yf
import pandas as pd


def yf_first(isi: list):
    starter = yf.download(isi,period="5y", interval="1wk") 
    hasil = {}
    for i in starter['Close']:
        julia = starter['Close'][i].dropna()
        nalia = []
        for kukukaka in range(len(julia)):
            nalia.append({"Timestamp": julia.index[kukukaka].strftime("%Y/%m/%d"), "Close": julia[kukukaka].item()})
        hasil[i] = nalia 
    return(hasil)

def data_ingest_run(isi: list):
    info = yf.Tickers(isi)
    hasil = {}
    try:
        historis = info.history(period='1d', interval='1m', progress=False, repair=True)
        reynauld = historis.index[-1].strftime("%Y/%m/%d")
        for i in historis['Close']:
            dismas = historis['Close'][i].iloc[-1]
            hasil[i] = [{"Timestamp": reynauld, "Close": dismas}]
    except:
        print("mencoba mengulang server")
        data_ingest_run(isi)
    return(hasil)

if __name__ == '__main__':
    indes = ["BTC-USD", "XRP-USD", "NVAX"]
    data_ingest_run(indes)
