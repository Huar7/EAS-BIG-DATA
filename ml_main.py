import pandas as pd
from sqlalchemy.engine.base import Engine
from statsmodels.tsa.arima.model import ARIMA
from psycopg2.extensions import connection


def arima_ml(engine: Engine, con: connection):
    """Ini adalah fungsi yang akan dijalankan untuk mengambil data dari postgres untuk dikirimkan dan dilakukan fungsi Arima"""
    df = pd.read_sql_query("select * from data_kotor", con=engine)
    cpl = {}
    deja = df.drop(columns="Timestamp")
    for i in deja:
        mdl = ARIMA(deja[i], order=(1, 1, 1))
        mdlft = mdl.fit()

        cpl[i] = mdlft.forecast(steps=3)
    cur = con.cursor()
    cur.execute("drop table if exists data_prediksi;")
    con.commit()
    daf = pd.DataFrame(cpl)
    daf.to_sql("data_prediksi", engine, index=False)
    cur.close()

