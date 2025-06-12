import psycopg2 as psy

def main():
    con = psy.connect(
        host="localhost",
        database="NurHary",
        user="NurHary",
        password="ForourDreams",
        port="5431",
    )

    cur = con.cursor()
    cur.execute("drop table if exist gross_value, arima_value")
    con.close()

if __name__ == "__main__":
    main()
