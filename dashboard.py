import streamlit as st

import wkaf as kw
import spark_builder as sb


def change_time():
    print(
        "===============================================================",
        st.session_state.period,
    )


pilihan_waktu = ["1 jam", "1 hari", "5 hari", "1 minggu", "1 bulan"]
periode_sel = {
    "1 jam": "1h",
    "1 hari": "1d",
    "5 hari": "5d",
    "1 minggu": "1wk",
    "1 bulan": "1mo",
}


def toggle_stream():
    st.session_state.time = not st.session_state.time


if "data" not in st.session_state:
    st.session_state.data = []
    st.session_state.itter = 0


if "time" not in st.session_state:
    st.session_state.time = False

st.sidebar.title("Control Center")
st.sidebar.button("Stream Data", disabled=st.session_state.time, on_click=toggle_stream)
st.sidebar.button(
    "Stop Stream Data", disabled=not st.session_state.time, on_click=toggle_stream
)

with st.sidebar.expander("Pilih Periode data"):
    period_arr = st.radio(
        "",
        pilihan_waktu,
        horizontal=False,
    )
    period = periode_sel[period_arr]
    st.session_state.period = period  # // it just work?
    change_time()


# Result
st.sidebar.text(f"{period}")


def run_fn():
    print("sukses11")


def run_fn2():
    print("sukses12")


@st.fragment(run_every=60)
def fragment_d_data():
    # sb.Rdata(st.session_state.itter)
    st.session_state.itter += 1


@st.fragment(run_every=0.5)
def fragment_g_data():
    pass


# // ini untuk melakukan iterasi setiap beberapa detik
fragment_g_data()
fragment_d_data()
