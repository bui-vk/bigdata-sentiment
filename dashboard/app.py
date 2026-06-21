import streamlit as st
from pyhive import hive
import pandas as pd

st.title("FINTEL — Sentiment Dashboard")

conn = hive.Connection(host="hive-server", port=10000, database="sentiment")
df = pd.read_sql("SELECT * FROM company_sentiment", conn)
st.dataframe(df)