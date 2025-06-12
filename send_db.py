import psycopg2 as psy
import pandas as pd
from sqlalchemy.engine.base import Engine


def send_val(data: pd.DataFrame, engine: Engine):
    """Fungsi ini ada untuk mengirim data pandas ke postgresql"""

    con = psy.connect(
        host="localhost",
        database="NurHary",
        user="NurHary",
        password="ForourDreams",
        port="5431",
    )
    cur = con.cursor()
    cur.execute("drop table if exists data_kotor;")
    con.commit()

    data.to_sql("data_kotor", engine, index=False)
    cur.close()
    con.close()
