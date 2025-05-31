# Final Project BIG DATA
## Tentang Project
Project ini memiliki beragam file dengan fungsi mereka masing - masing (yang kemungkinan akan di update dan dijadikan dalam satu file).

1. get_trending.py, fungsi utama file ini adalah untuk melakukan scrapping pada website yahoo finance trending untuk mendapatkan simbol perusahaan dengan stock tertinggi (akan di deprecated)
2. main2.py, fungsi utama file ini adalah untuk melakukan operasi dengan library yfinance dan mengembalikan nilai hasil download dari yfinance. terdapat dua jenis yang dikirimkan dari file tersebut: 
    - first run (yf_first()), ini menggunakan yfinance.download untuk mendapatkan data historis
    - stream run (data_ingest_run()), ini menggunakan yfinance.Tickers untuk mendapatkan data terkini
3. main.py, fungsi utama dari file ini adalah untuk melakukan looping untuk menjalankan fungsi data_ingest_run() yang terus menerus menulis hasil download data tersebut ke kafka
4. spark_builder.py, fungsi utama dari file ini adalah untuk menerima data kakfa, mengencodenya, dan membersihkan data tersebut untuk dijadikan dalam bentuk pyspark.DataFrame sembari melakukan machine learning dan mengirimkan data tersebut ke dalam database postgresql dengan menggunakan psycopg2
*on Work*
5. ..., fungsi utama dari file ini adalah untuk membuat dashboard, menerima nilai dari postgresql dengan psycopg2, dan fungsi - fungsi lainnya pada dashboard



## Todo:
- [ ] membuat docker compose yaml
- [x] top_stock(): menambahkan fungsi yang akan melakukan scrapping pada website yahoo finance untuk mendapatkan simbol trend untuk stock market
- [x] datastream_normalization(): Menambahkan fungsi untuk mengubah model data streamed menjadi struktur yang dapat diterima oleh pyspark
- [ ] menambahkan sistem pengiriman data yang dibuat oleh pyspark dan data prediksi pyspark ke postgres (Pasial)
- [ ] membuat daashboard


## Hal yang harus diperbaiki
- [ ] interval download untuk data seragam, gunakan interval per menit (sangat boros)
- [ ] occlusion culling ("simpan data apabila dibutuhkan / dilihat, jika tidak hapus!") pada data
