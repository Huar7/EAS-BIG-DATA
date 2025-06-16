# Final Project BIG DATA
## Tentang Project
Project ini memiliki beragam file dengan fungsi mereka masing - masing (yang kemungkinan akan di update dan dijadikan dalam satu file).

## File dan Fungsi - Fungsi nya
1. wkaf.py: fungsi dari file python ini adalah untuk melakukan scrapping data secara langsung dari yahoo finance melalui yfinance, dan juga untuk melakukan pengiriman data tersebut melalui kafka producer
2. spark_builder: fungsi dari file python ini adalah untuk menerima data dari kafka dan membangung spark data frame yang kemudian dilakukan pengiriman pada fungsi send_db
3. send_db: adalah file dengan fungsi utama untuk mengirim data ke postgresql
4. ml_main: fungsi utama dari file ini adalah untuk melakukan prediksi ARIMA
5. dashboard.py: adalah file yang membangun dashboard dengan menggunakan Streamlit
