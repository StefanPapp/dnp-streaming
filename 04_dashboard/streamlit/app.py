"""Streamlit dashboard for Reddit signal metrics stored in TimescaleDB."""

import os

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
from streamlit_autorefresh import st_autorefresh

# Connection resolved from the environment (set in docker-compose), defaulting
# to the compose `timescaledb` service with the dev credentials.
DB_URL = os.environ.get(
    "DB_URL",
    "postgresql+psycopg2://reddit:reddit@timescaledb:5432/reddit",
)

# NOTE: this assumes a `reddit_posts` table fed from the reddit-posts topic.
# The schema does not exist yet — create it (and `mentions_hourly`) before the
# queries below will return data. Column names here mirror the worker payload.
TOP_POSTS_SQL = """
    SELECT created_utc, title, author, score, num_comments, url
    FROM reddit_posts
    WHERE subreddit = %(sub)s
      AND created_utc > NOW() - make_interval(hours => %(h)s)
    ORDER BY score DESC
    LIMIT 20
"""

st.set_page_config(page_title="Reddit Signal Dashboard", layout="wide")
engine = create_engine(DB_URL)

st.title("Reddit Signal Dashboard")
col1, col2 = st.columns([1, 3])
with col1:
    subreddit = st.selectbox("Subreddit", ["BMW", "Volkswagen", "Mercedes"])
    hours = st.slider("Zeitfenster (Stunden)", 6, 168, 48)

query = """
    SELECT hour, mention_count, avg_score
    FROM mentions_hourly
    WHERE subreddit = %(sub)s AND hour > NOW() - INTERVAL %(h)s
    ORDER BY hour
"""
df = pd.read_sql(query, engine, params={"sub": subreddit, "h": f"{hours} hours"})

st.subheader(f"Mentions pro Stunde — r/{subreddit}")
st.line_chart(df.set_index("hour")["mention_count"])

st.subheader("Top-Posts der Periode")
top = pd.read_sql(TOP_POSTS_SQL, engine, params={"sub": subreddit, "h": hours})
st.dataframe(top)

st_autorefresh(interval=30_000, key="refresh")
