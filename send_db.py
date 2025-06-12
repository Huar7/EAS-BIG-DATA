import psycopg2 as psy
from pyspark.sql.dataframe import DataFrame

def send_to_postgres(iter: int, data: DataFrame):
    err_val = 1
    con = psy.connect(
        host="localhost",
        database="NurHary",
        user="NurHary",
        password="ForourDreams",
        port="5431",
    )
    cur = con.cursor()

    if iter == 0:
        try:
            pass
        except:
            pass
        pass
    else:
        pass
    pass
