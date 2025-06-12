# Final Project BIG DATA
## Tentang Project
Project ini memiliki beragam file dengan fungsi mereka masing - masing (yang kemungkinan akan di update dan dijadikan dalam satu file).

1. wkaf.py, fungsi utama dari file ini adalah untuk mendapatkan data dan mengirimnya langsung ke kafka
2. spark_builder.py, fungsi utama dari file ini adalah untuk menerima data kakfa, mengencodenya, dan membersihkan data tersebut untuk dijadikan dalam bentuk pyspark.DataFrame sembari melakukan machine learning dan mengirimkan data tersebut ke dalam database postgresql dengan menggunakan psycopg2
3. ml_main.py, fungsi dari file ini untuk menerima data yang telah di masukkan kedalam tabel di postgresql dan melakukan prediksi dengan mengguanakan ARIMA untuk memprediksi nilai harga dan mengirimnya ke Dashboard
4. Dashboard.py, 


## Todo:
- [x] top_stock(): menambahkan fungsi yang akan melakukan scrapping pada website yahoo finance untuk mendapatkan simbol trend untuk stock market
- [x] datastream_normalization(): Menambahkan fungsi untuk mengubah model data streamed menjadi struktur yang dapat diterima oleh pyspark
- [x] menambahkan sistem pengiriman data yang dibuat oleh pyspark dan data prediksi pyspark ke postgres (Pasial)
- [ ] membuat daashboard


## Hal yang harus diperbaiki
- [x] interval download untuk data seragam, gunakan interval per menit (sangat boros)

## Main - Task
- [x] Membuat send_db dengan isi untuk melakukan pengiriman pada database (via psycopg2) yang akan dijalankan setiap Rdata return 0
- [ ] Mencari tahu cara melakukan ML
- [ ] Membuat main_ml yang akan dijalankan setiap Rdata return 0
