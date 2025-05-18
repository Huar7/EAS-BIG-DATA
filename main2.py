import time
import yfinance as yf
import pandas as pd

info2 = yf.Tickers(["MSFT"])
histor = info2.history(period='1d', interval='1m')
#print(histor["Close"].iloc[-1])

def yf_continous(isi: list):
    return yf.Tickers(isi)

def yf_first(isi: list):
    return yf.download(isi,period="5y", interval="1wk") 

def data_ingest_run():
    indes = ["BTC-USD", "XRP-USD", "NVAX"]
    starter = yf_first(indes)

    for i in starter['Close']:
        julia = starter['Close'][i].dropna()
    while True:
        info = yf_continous(indes)
        try:
            historis = info.history(period='1d', interval='1m', progress=False, repair=True)
            reynauld = historis.index[-1]
            time_point = ""
            if reynauld != time_point:
                for i in historis['Close']:
                    if pd.isna(historis["Close"][i].iloc[-1]):
                        print("Melompati Karena Kosong", i)
                    else:
                            dismas = historis['Close'][i].iloc[-1]
                            print(i, dismas, reynauld)
                time_point = reynauld
            else:
                print("Melompati, Dikarenakan tidak ada update")
        except:
           print(f"mencoba mengulang server")
           time.sleep(60)

if __name__ == '__main__':
    data_ingest_run()
