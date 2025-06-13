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
    cur.execute("drop table if exists data_prediksi, data_kotor, jajal2, jajal")
    con.commit()
    print("sukses 1")
    cur.close()
    con.close()

if __name__ == "__main__":
    main()
