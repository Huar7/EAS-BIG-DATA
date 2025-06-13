from datetime import datetime

import kafka as kf
import spark_builder as sb
import streamlit as st
import wkaf as kw
from pyspark.sql import SparkSession
import json

from sqlalchemy import create_engine

import numpy as np

import pandas as pd

empty = st.empty()


def show_data():
    df = st.session_state.data
    if df is not None:
        numeric_cols = df.select_dtypes(include="number").columns
        with empty.container():
            st.line_chart(df[numeric_cols], use_container_width=True)
    else:
        print("Empty")


indes = kw.trending()

engine = create_engine("postgresql://NurHary:ForourDreams@localhost:5431/NurHary")
# // inisialisasi Spark dan consumer disini untuk mempercepat sistem kerja fungsi Rdata
# // untuk kafka sendiri supaya dapat menerima nilai dengan cara fungsi recursive seperti yang dapat kita lihat dibawahjal
spark = SparkSession.builder.appName("main_app").getOrCreate()

consumer = kf.KafkaConsumer(
    "stock_kotor",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="latest",  # // ini untuk model pengambilan data: latest untuk mengambil data yang baru di luncurkan
    enable_auto_commit=True,  # //
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)
consumer.subscribe(topics=["stock_kotor"])

pilihan_waktu = ["1 hari", "5 hari", "1 bulan"]
periode_sel = {
    "1 hari": "1d",
    "5 hari": "5d",
    "1 bulan": "1mo",
}

if "data" not in st.session_state:
    st.session_state.data = None
    st.session_state.pred = None
    show_data()
    st.session_state.itter = 0
    st.session_state.nil_one = {}
    st.session_state.dfer = None
    st.session_state.unwanted_list = []

if "time" not in st.session_state:
    st.session_state.time = False


def toggle_stream():
    st.session_state.time = not st.session_state.time


def change_time():
    st.session_state.itter = 0


st.sidebar.title("Control Center")
st.sidebar.button("Stream Data", disabled=st.session_state.time, on_click=toggle_stream)
st.sidebar.button(
    "Stop Stream Data", disabled=not st.session_state.time, on_click=toggle_stream
)

with st.sidebar.expander("Pilih Periode data"):
    period_arr = st.radio("", pilihan_waktu, horizontal=False, on_change=change_time)
    period = periode_sel[period_arr]
    st.session_state.period = period  # // it just work?

if st.session_state.time is True:
    run_get = 5.0
else:
    run_get = None


@st.fragment(run_every=0.1)
def fragment_receive_data():
    # // dilakukan run non stop untuk menerima nilai yang ada,

    # // bagaimana cara memberhentikan waktunya ketika tidak ada data yang diterima?
    # // mungkin gak usah, tapi aku ingin sekali
    # print("luh ngeloop")
    kelanjutan = sb.Rdata(
        st.session_state.itter,
        consumer,
        st.session_state.nil_one,
        spark,
        engine,
        st.session_state.dfer,
        st.session_state.unwanted_list,
    )
    print(st.session_state.time, st.session_state.itter)
    if kelanjutan[0] == 0:
        st.session_state.itter += 1
        st.session_state.nil_one = kelanjutan[1]
        st.session_state.dfer = kelanjutan[2]
        st.session_state.unwanted_list = kelanjutan[3]

        st.session_state.data = pd.read_sql_query(
            "select * from data_kotor", con=engine
        )
        st.session_state.pred = pd.read_sql_query(
            "select * from data_prediksi", con=engine
        )

        print("\n \n")
        print(st.session_state.data.index[-1])
        print("\n \n")

        show_data()

        # // ini kita akan isi dengan pengiriman pada


@st.fragment(run_every=run_get)
def fragment_get_data():
    # // Jadi saya harus memulai fungsi ini untuk
    kw.Wdata(st.session_state.period, st.session_state.itter, indes)


# // ini untuk melakukan iterasi setiap beberapa detik
fragment_receive_data()
fragment_get_data()
