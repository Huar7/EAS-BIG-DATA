from logging import disable
import streamlit as st
import wkaf as kw
import spark_builder as sb
from calendar import month_abbr as mnthabr
from datetime import datetime


def toggle_stream():
    st.session_state.time = not st.session_state.time


if "data" not in st.session_state:
    st.session_state.data = []


if "time" not in st.session_state:
    st.session_state.time = False

st.sidebar.title("Control Center")
st.sidebar.button("Stream Data", disabled=st.session_state.time, on_click=toggle_stream)
st.sidebar.button(
    "Stop Stream Data", disabled=not st.session_state.time, on_click=toggle_stream
)

st.sidebar.slider("Kecepatan Streaming", 30.0, 60.0, value=45.0, key="run_stream")
st.sidebar.slider("Kecepatan receiver", 0.25, 1.0, value=0.5, key="run_receiv")

with st.sidebar.expander("Report month"):
    this_year = datetime.now().year
    this_month = datetime.now().month
    report_year = st.selectbox(
        "", range(this_year, this_year - 10, -1), disabled=st.session_state.time
    )
    month_abbr = mnthabr[1:]
    report_month_str = st.radio(
        "",
        month_abbr,
        index=this_month - 1,
        horizontal=True,
        disabled=st.session_state.time,
    )
    report_month = month_abbr.index(report_month_str) + 1  # // Hasil

# Result
st.sidebar.text(f"{report_year} {report_month_str}, {report_month}")
if st.session_state.time is True:
    run_stream = st.session_state.run_stream
    run_receiv = st.session_state.run_receiv
    print("mie sukses isi 5")
else:
    run_stream = None
    run_receiv = None


def run_fn():
    print("sukses11")


def run_fn2():
    print("sukses12")


@st.fragment(run_every=run_stream)
def fragment_d_data():
    run_fn()


@st.fragment(run_every=run_receiv)
def fragment_g_data():
    run_fn2()


fragment_g_data()
fragment_d_data()
