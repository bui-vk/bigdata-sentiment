import ast
import streamlit as st
from pyhive import hive
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px

st.set_page_config(
    page_title="FINTEL",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');
html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }
.fintel-header { background:#0a0a0f; border-bottom:1px solid #1e3a5f; padding:1.2rem 2rem 1rem; margin:-1rem -1rem 2rem; }
.fintel-title  { font-family:'IBM Plex Mono',monospace; font-size:1.6rem; font-weight:600; color:#00d4ff; letter-spacing:0.08em; margin:0; }
.fintel-subtitle { font-family:'IBM Plex Mono',monospace; font-size:0.7rem; color:#4a7fa5; margin:0.2rem 0 0; letter-spacing:0.12em; text-transform:uppercase; }
.badge-positive { background:#0d3b2e; color:#00e676; padding:2px 10px; border-radius:12px; font-size:0.75rem; font-weight:600; }
.badge-negative { background:#3b0d0d; color:#ff5252; padding:2px 10px; border-radius:12px; font-size:0.75rem; font-weight:600; }
.badge-neutral  { background:#1e1e2e; color:#90a4ae; padding:2px 10px; border-radius:12px; font-size:0.75rem; font-weight:600; }
</style>
""", unsafe_allow_html=True)

NAME_FIX     = {"NVDA": "NVIDIA Corporation", "SHW": "The Sherwin-Williams Company"}
INDUSTRY_FIX = {"NVDA": "Technology", "SHW": "Materials"}

def parse_headlines(raw):
    if raw is None:
        return []
    if isinstance(raw, list):
        return [h for h in raw if h]
    try:
        parsed = ast.literal_eval(str(raw))
        if isinstance(parsed, list):
            return [h for h in parsed if h]
    except Exception:
        pass
    return [str(raw)] if str(raw).strip() else []

@st.cache_resource
def get_conn():
    return hive.Connection(host="hive-server", port=10000, database="sentiment")

@st.cache_data(ttl=300)
def load_sentiment():
    conn = get_conn()
    df = pd.read_sql("SELECT * FROM company_sentiment", conn)
    df.columns = [c.replace("company_sentiment.", "") for c in df.columns]
    df["company_name"] = df["company_name"].replace(NAME_FIX)
    df["industry"]     = df["industry"].replace(INDUSTRY_FIX)
    df["event_date"]   = pd.to_datetime(df["event_date"])
    return df

@st.cache_data(ttl=300)
def load_prices():
    conn = get_conn()
    df = pd.read_sql("SELECT * FROM price_data", conn)
    df.columns = [c.replace("price_data.", "") for c in df.columns]
    df["event_date"] = pd.to_datetime(df["event_date"])
    return df

@st.cache_data(ttl=300)
def load_correlation():
    conn = get_conn()
    df = pd.read_sql("SELECT * FROM sentiment_price_correlation", conn)
    df.columns = [c.replace("sentiment_price_correlation.", "") for c in df.columns]
    df["company_name"] = df["company_name"].replace(NAME_FIX)
    df["industry"]     = df["industry"].replace(INDUSTRY_FIX)
    df["event_date"]   = pd.to_datetime(df["event_date"])
    return df

@st.cache_data(ttl=60)
def load_pipeline_runs():
    conn = get_conn()
    df = pd.read_sql(
        "SELECT run_date, status, articles_processed, errors FROM pipeline_runs ORDER BY run_date DESC",
        conn,
    )
    df.columns = [c.replace("pipeline_runs.", "") for c in df.columns]
    return df

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.markdown("""
<div class="fintel-header">
  <p class="fintel-title">FINTEL</p>
  <p class="fintel-subtitle">Financial Sentiment Intelligence · DJI-32 · Real-time Pipeline</p>
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
with st.spinner("Loading data from Hive..."):
    try:
        sentiment_df = load_sentiment()
        prices_df    = load_prices()
        corr_df      = load_correlation()
        runs_df      = load_pipeline_runs()
        data_ok = True
    except Exception as e:
        st.error(f"Could not connect to Hive: {e}")
        st.info("Make sure the hive-server container is running and the pipeline has completed at least one run.")
        data_ok = False

if not data_ok:
    st.stop()

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("### Filters")
    all_industries = sorted(sentiment_df["industry"].dropna().unique())
    selected_industries = st.multiselect("Industry", all_industries, default=all_industries)

    date_min = sentiment_df["event_date"].min().date()
    date_max = sentiment_df["event_date"].max().date()
    date_range = st.date_input("Date range", value=(date_min, date_max),
                               min_value=date_min, max_value=date_max)
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date = pd.Timestamp(date_range[0])
        end_date   = pd.Timestamp(date_range[1])
    else:
        start_date = pd.Timestamp(date_min)
        end_date   = pd.Timestamp(date_max)

    st.markdown("---")
    st.markdown("### About")
    st.markdown(
        "FINTEL aggregates financial news via NewsAPI, scores it with FinBERT, "
        "and correlates sentiment with Yahoo Finance price data across the DJI-32 index."
    )
    latest = sentiment_df["event_date"].max()
    st.caption(f"Latest data: **{latest.strftime('%Y-%m-%d')}**")

    st.markdown("---")
    st.markdown("### Pipeline Status")
    if not runs_df.empty:
        last = runs_df.iloc[0]
        status_color = "#00e676" if last["status"] == "success" else "#ff5252"
        st.markdown(
            f"<span style='color:{status_color};font-weight:600'>● {last['status'].upper()}</span> "
            f"<span style='color:#4a7fa5;font-size:0.8rem'>{last['run_date']}</span>",
            unsafe_allow_html=True,
        )
        st.caption(f"{int(last['articles_processed'])} articles processed")
        if last.get("errors"):
            st.caption(f"⚠️ {last['errors']}")
        if len(runs_df) > 1:
            with st.expander("Run history"):
                st.dataframe(runs_df[["run_date", "status", "articles_processed"]],
                             hide_index=True, use_container_width=True)
    else:
        st.caption("No pipeline runs logged yet.")

# ---------------------------------------------------------------------------
# Apply filters
# ---------------------------------------------------------------------------
mask = (
    sentiment_df["industry"].isin(selected_industries) &
    (sentiment_df["event_date"] >= start_date) &
    (sentiment_df["event_date"] <= end_date)
)
filtered = sentiment_df[mask]

latest_per_company = (
    filtered.sort_values("event_date", ascending=False)
    .groupby("company_name", as_index=False)
    .first()
    .sort_values("sentiment_score", ascending=False)
)

# ---------------------------------------------------------------------------
# Top-level metrics
# ---------------------------------------------------------------------------
n_companies    = latest_per_company["company_name"].nunique()
n_positive     = (latest_per_company["sentiment_label"] == "positive").sum()
n_negative     = (latest_per_company["sentiment_label"] == "negative").sum()
avg_score      = latest_per_company["sentiment_score"].mean()
total_mentions = filtered["mention_count"].sum()

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Companies tracked",  n_companies)
c2.metric("Positive sentiment", n_positive)
c3.metric("Negative sentiment", n_negative)
c4.metric("Avg sentiment score", f"{avg_score:.3f}")
c5.metric("Total mentions", f"{int(total_mentions):,}")

st.markdown("---")

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab_rankings, tab_company, tab_correlation = st.tabs([
    "📊 Rankings", "🔍 Company Detail", "📈 Sentiment vs Price"
])

# ── Rankings ────────────────────────────────────────────────────────────────
with tab_rankings:
    st.subheader("Company Sentiment Rankings")
    st.caption(f"Most recent available day per company · {len(latest_per_company)} companies")

    display = latest_per_company[[
        "company_name", "industry", "sentiment_label",
        "sentiment_score", "mention_count", "pct_positive", "pct_negative", "event_date"
    ]].copy()
    display["event_date"]      = display["event_date"].dt.strftime("%Y-%m-%d")
    display["sentiment_score"] = display["sentiment_score"].round(4)
    display["pct_positive"]    = (display["pct_positive"] * 100).round(1)
    display["pct_negative"]    = (display["pct_negative"] * 100).round(1)

    fig_bar = px.bar(
        display.sort_values("sentiment_score"),
        x="sentiment_score", y="company_name",
        color="sentiment_score",
        color_continuous_scale=["#ff5252", "#90a4ae", "#00e676"],
        color_continuous_midpoint=0, orientation="h",
        labels={"sentiment_score": "Sentiment Score", "company_name": ""},
        height=max(400, len(display) * 22),
    )
    fig_bar.update_layout(
        paper_bgcolor="#0a0a0f", plot_bgcolor="#111827", font_color="#e2e8f0",
        coloraxis_showscale=False, margin=dict(l=0, r=20, t=10, b=20),
        xaxis=dict(gridcolor="#1e3a5f", zerolinecolor="#2d5a8a"),
        yaxis=dict(gridcolor="rgba(0,0,0,0)"),
    )
    st.plotly_chart(fig_bar, use_container_width=True)

    def badge(label):
        cls = {"positive": "badge-positive", "negative": "badge-negative"}.get(label, "badge-neutral")
        return f'<span class="{cls}">{label}</span>'

    rows_html = ""
    for _, r in display.iterrows():
        rows_html += f"""<tr>
          <td style="padding:6px 12px">{r['company_name']}</td>
          <td style="padding:6px 12px;color:#4a7fa5">{r['industry']}</td>
          <td style="padding:6px 12px">{badge(r['sentiment_label'])}</td>
          <td style="padding:6px 12px;font-family:monospace">{r['sentiment_score']:+.4f}</td>
          <td style="padding:6px 12px">{int(r['mention_count'])}</td>
          <td style="padding:6px 12px;color:#00e676">{r['pct_positive']:.1f}%</td>
          <td style="padding:6px 12px;color:#ff5252">{r['pct_negative']:.1f}%</td>
          <td style="padding:6px 12px;color:#4a7fa5">{r['event_date']}</td>
        </tr>"""

    st.markdown(f"""
    <table style="width:100%;border-collapse:collapse;background:#111827;border-radius:8px;overflow:hidden">
      <thead><tr style="background:#0a0a0f;color:#4a7fa5;font-size:0.75rem;text-transform:uppercase;letter-spacing:0.08em">
        <th style="padding:8px 12px;text-align:left">Company</th>
        <th style="padding:8px 12px;text-align:left">Industry</th>
        <th style="padding:8px 12px;text-align:left">Sentiment</th>
        <th style="padding:8px 12px;text-align:left">Score</th>
        <th style="padding:8px 12px;text-align:left">Mentions</th>
        <th style="padding:8px 12px;text-align:left">% Positive</th>
        <th style="padding:8px 12px;text-align:left">% Negative</th>
        <th style="padding:8px 12px;text-align:left">As of</th>
      </tr></thead>
      <tbody style="color:#e2e8f0;font-size:0.85rem">{rows_html}</tbody>
    </table>""", unsafe_allow_html=True)

# ── Company Detail ───────────────────────────────────────────────────────────
with tab_company:
    companies        = sorted(latest_per_company["company_name"].unique())
    selected_company = st.selectbox("Select company", companies)
    company_data     = filtered[filtered["company_name"] == selected_company].sort_values("event_date")
    latest_row       = company_data.iloc[-1] if len(company_data) > 0 else None

    if latest_row is None:
        st.warning("No data for this company in the selected date range.")
    else:
        ca, cb, cc, cd = st.columns(4)
        ca.metric("Sentiment score", f"{latest_row['sentiment_score']:+.4f}")
        cb.metric("Sentiment label", latest_row["sentiment_label"].capitalize())
        cc.metric("Mention count",   int(latest_row["mention_count"]))
        cd.metric("% Positive",      f"{latest_row['pct_positive']*100:.1f}%")

        st.markdown("#### Sentiment Score Over Time")
        fig_trend = go.Figure()
        fig_trend.add_trace(go.Scatter(
            x=company_data["event_date"], y=company_data["sentiment_score"],
            mode="lines+markers", line=dict(color="#00d4ff", width=2), marker=dict(size=5),
            fill="tozeroy", fillcolor="rgba(0,212,255,0.06)",
        ))
        fig_trend.add_hline(y=0, line_dash="dot", line_color="#4a7fa5", line_width=1)
        fig_trend.update_layout(
            paper_bgcolor="#0a0a0f", plot_bgcolor="#111827", font_color="#e2e8f0",
            height=280, margin=dict(l=0, r=0, t=10, b=0),
            xaxis=dict(gridcolor="#1e3a5f"),
            yaxis=dict(gridcolor="#1e3a5f", title="Score"),
        )
        st.plotly_chart(fig_trend, use_container_width=True)

        st.markdown("#### Daily Mention Count")
        fig_mentions = px.bar(company_data, x="event_date", y="mention_count",
                              color_discrete_sequence=["#1e3a5f"])
        fig_mentions.update_layout(
            paper_bgcolor="#0a0a0f", plot_bgcolor="#111827", font_color="#e2e8f0",
            height=200, margin=dict(l=0, r=0, t=10, b=0),
            xaxis=dict(gridcolor="#1e3a5f", title=""),
            yaxis=dict(gridcolor="#1e3a5f", title="Mentions"), showlegend=False,
        )
        st.plotly_chart(fig_mentions, use_container_width=True)

        st.markdown(
            f"#### Recent Headlines "
            f"<span style='color:#4a7fa5;font-size:0.8rem'>({latest_row['event_date'].strftime('%Y-%m-%d')})</span>",
            unsafe_allow_html=True,
        )
        headlines = parse_headlines(latest_row.get("sample_headlines"))
        if headlines:
            for h in headlines:
                st.markdown(f"- {h}")
        else:
            st.caption("No headlines stored for this date.")

# ── Sentiment vs Price ───────────────────────────────────────────────────────
with tab_correlation:
    st.subheader("Sentiment vs. Stock Price")
    st.caption("Dual-axis view: left = sentiment score, right = close price. 7-day rolling correlation below.")

    corr_companies = sorted(corr_df["company_name"].dropna().unique())
    corr_company   = st.selectbox("Select company", corr_companies, key="corr_co")

    COMPANY_TICKER = {r["company_name"]: r.get("ticker")
                      for _, r in latest_per_company.iterrows()}
    ticker       = COMPANY_TICKER.get(corr_company)
    sent_series  = corr_df[corr_df["company_name"] == corr_company].sort_values("event_date")
    price_series = prices_df[prices_df["ticker"] == ticker].sort_values("event_date") \
                   if ticker else pd.DataFrame()

    if sent_series.empty:
        st.warning("No correlation data for this company.")
    else:
        fig_dual = make_subplots(specs=[[{"secondary_y": True}]])
        fig_dual.add_trace(go.Scatter(
            x=sent_series["event_date"], y=sent_series["sentiment_score"],
            name="Sentiment Score", mode="lines+markers",
            line=dict(color="#00d4ff", width=2),
        ), secondary_y=False)
        if not price_series.empty:
            fig_dual.add_trace(go.Scatter(
                x=price_series["event_date"], y=price_series["close_price"],
                name="Close Price (USD)", mode="lines",
                line=dict(color="#ffd700", width=2, dash="dot"),
            ), secondary_y=True)
        fig_dual.add_hline(y=0, line_dash="dot", line_color="#4a7fa5", line_width=1, secondary_y=False)
        fig_dual.update_layout(
            paper_bgcolor="#0a0a0f", plot_bgcolor="#111827", font_color="#e2e8f0",
            height=380, margin=dict(l=0, r=0, t=20, b=0),
            legend=dict(bgcolor="#111827", bordercolor="#1e3a5f"),
            xaxis=dict(gridcolor="#1e3a5f"),
        )
        fig_dual.update_yaxes(title_text="Sentiment Score", gridcolor="#1e3a5f", secondary_y=False)
        fig_dual.update_yaxes(title_text="Close Price (USD)", gridcolor="rgba(0,0,0,0)", secondary_y=True)
        st.plotly_chart(fig_dual, use_container_width=True)

        st.markdown("#### 7-Day Rolling Correlation")
        fig_corr = go.Figure()
        fig_corr.add_trace(go.Scatter(
            x=sent_series["event_date"], y=sent_series["correlation_7d"],
            mode="lines+markers", line=dict(color="#a78bfa", width=2),
            fill="tozeroy", fillcolor="rgba(167,139,250,0.08)",
        ))
        fig_corr.add_hline(y=0, line_dash="dot", line_color="#4a7fa5", line_width=1)
        fig_corr.update_layout(
            paper_bgcolor="#0a0a0f", plot_bgcolor="#111827", font_color="#e2e8f0",
            height=220, margin=dict(l=0, r=0, t=10, b=0),
            xaxis=dict(gridcolor="#1e3a5f"),
            yaxis=dict(gridcolor="#1e3a5f", title="Corr", range=[-1.1, 1.1]),
        )
        st.plotly_chart(fig_corr, use_container_width=True)

        mean_corr = sent_series["correlation_7d"].mean()
        direction = ("positive" if mean_corr > 0.1
                     else "negative" if mean_corr < -0.1 else "near-zero")
        st.info(
            f"**{corr_company}** — mean 7-day correlation: **{mean_corr:+.3f}** ({direction}). "
            + ("Positive news aligns with rising prices." if direction == "positive"
               else "Positive news diverges from price movement." if direction == "negative"
               else "No strong alignment between sentiment and price movement.")
        )

    st.markdown("#### All Companies — Mean 7-Day Correlation")
    corr_summary = (
        corr_df.groupby("company_name")["correlation_7d"].mean().round(4).reset_index()
        .sort_values("correlation_7d", ascending=False)
        .rename(columns={"company_name": "Company", "correlation_7d": "Mean Corr (7d)"})
    )
    fig_all = px.bar(
        corr_summary.sort_values("Mean Corr (7d)"),
        x="Mean Corr (7d)", y="Company",
        color="Mean Corr (7d)",
        color_continuous_scale=["#ff5252", "#90a4ae", "#00e676"],
        color_continuous_midpoint=0, orientation="h",
        height=max(350, len(corr_summary) * 22),
    )
    fig_all.update_layout(
        paper_bgcolor="#0a0a0f", plot_bgcolor="#111827", font_color="#e2e8f0",
        coloraxis_showscale=False, margin=dict(l=0, r=20, t=10, b=20),
        xaxis=dict(gridcolor="#1e3a5f", zerolinecolor="#2d5a8a", range=[-1.1, 1.1]),
        yaxis=dict(gridcolor="rgba(0,0,0,0)"),
    )
    st.plotly_chart(fig_all, use_container_width=True)