from datetime import datetime

from streamlit_dynamic_filters import DynamicFilters
import kafka as kf
import spark_builder as sb
import streamlit as st
import altair as at
import wkaf as kw
from pyspark.sql import SparkSession
import json

from sqlalchemy import create_engine

import numpy as np
import pandas as pd


indes = kw.trending()
st.title("Trending Stock Data")


empty = st.empty()
empty2 = st.empty()
empty3 = st.empty()


def show_data():
    df = st.session_state.data
    if df is not None:
        dapo = st.session_state.pred
        dupu = dapo.iloc[-1] - df.iloc[-1]
        print(dupu)
        dupul = dupu.nlargest(5)
        dupu_n = dupul.index.to_list()
        dupu_v = dupul.to_list()

        dupus = dupu.nsmallest(5)
        dupus_n = dupus.index.to_list()
        dupus_v = dupus.to_list()

        numeric_cols = df.select_dtypes(include="number").columns

        # // Bagian pembuatan dan penggabungan dengan prediksi
        dafa = st.session_state.pred

        with empty.container():
            st.line_chart(df[st.session_state.filter_set], use_container_width=True)

        with empty2.container():
            with st.expander("Top Gainer"):
                col1, col2 = st.columns(2)
                with col1:
                    st.subheader(dupu_n[0])
                    st.subheader(dupu_n[1])
                    st.subheader(dupu_n[2])
                    st.subheader(dupu_n[3])
                    st.subheader(dupu_n[4])
                with col2:
                    st.subheader(dupu_v[0])
                    st.subheader(dupu_v[1])
                    st.subheader(dupu_v[2])
                    st.subheader(dupu_v[3])
                    st.subheader(dupu_v[4])
        with empty3.container():
            with st.expander("Top Looser"):
                col1, col2 = st.columns(2)
                with col1:
                    st.subheader(dupus_n[0])
                    st.subheader(dupus_n[1])
                    st.subheader(dupus_n[2])
                    st.subheader(dupus_n[3])
                    st.subheader(dupus_n[4])
                with col2:
                    st.subheader(dupus_v[0])
                    st.subheader(dupus_v[1])
                    st.subheader(dupus_v[2])
                    st.subheader(dupus_v[3])
                    st.subheader(dupus_v[4])

    else:
        with empty.container():
            st.line_chart()


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

pilihan_waktu = ["1 Hari", "5 Hari", "1 Minggu", "1 Bulan", "3 Bulan", "6 Bulan"]
periode_sel = {
    "1 Hari": ["1d", "1m"],
    "5 Hari": ["5d", "1m"],
    "1 Minggu": ["1wk", "1m"],
    "1 Bulan": ["1mo", "5m"],
    "3 Bulan": ["3mo", "1h"],
    "6 Bulan": ["6mo", "1h"],
}


if "data" not in st.session_state:
    st.session_state.data = None
    st.session_state.pred = None
    st.session_state.filter_set = []
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
    period = periode_sel[period_arr][0]
    intv = periode_sel[period_arr][1]

    st.session_state.period = period  # // it just work?
    st.session_state.intv = intv  # // it just work?

if st.session_state.time is True:
    run_get = 60.0
else:
    run_get = None

with st.sidebar.expander("Filter"):
    if st.session_state.unwanted_list != []:
        indes.remove(st.session_state.unwanted_list)
    filter_set = st.multiselect("", indes, default=indes)
    st.session_state.filter_set = filter_set


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
        st.session_state.data.set_index("Timestamp", inplace=True)

        show_data()
        show_data()

        # // ini kita akan isi dengan pengiriman pada


@st.fragment(run_every=run_get)
def fragment_get_data():
    # // Jadi saya harus memulai fungsi ini untuk
    kw.Wdata(
        st.session_state.period, st.session_state.itter, indes, st.session_state.intv
    )


# // ini untuk melakukan iterasi setiap beberapa detik
fragment_receive_data()
fragment_get_data()
