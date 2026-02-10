import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

# ---------------- PAGE CONFIG ----------------
st.set_page_config(
    page_title="Financial Fraud Dashboard",
    layout="wide"
)

st.title("🚨 Financial Fraud Detection Dashboard")

# ---------------- SNOWFLAKE SESSION ----------------
session = get_active_session()

# ---------------- SAFE QUERY ----------------
@st.cache_data(ttl=60)
def run_query(query):
    try:
        return session.sql(query).to_pandas()
    except Exception as e:
        st.error(f"Query Failed: {e}")
        return pd.DataFrame()

# =====================================================
# KPI SECTION
# =====================================================
kpi_df = run_query("""
SELECT *
FROM FIN_FRAUD_DB.GOLD.VW_FRAUD_KPI
""")

if not kpi_df.empty:
    c1, c2, c3, c4, c5 = st.columns(5)

    c1.metric("Total Transactions", int(kpi_df.iloc[0]["TOTAL_TXNS"]))
    c2.metric("Total Amount", f"{kpi_df.iloc[0]['TOTAL_AMOUNT']:,.0f}")
    c3.metric("High Risk", int(kpi_df.iloc[0]["HIGH_RISK_TXNS"]))
    c4.metric("Medium Risk", int(kpi_df.iloc[0]["MEDIUM_RISK_TXNS"]))
    c5.metric("Low Risk", int(kpi_df.iloc[0]["LOW_RISK_TXNS"]))

st.divider()

# =====================================================
# LOAD TREND DATA
# =====================================================
trend_df = run_query("""
SELECT *
FROM FIN_FRAUD_DB.GOLD.VW_FRAUD_TRENDS
""")

if not trend_df.empty:

    trend_df["TXN_DATE"] = pd.to_datetime(trend_df["TXN_DATE"])
    trend_df["YEAR"] = trend_df["TXN_DATE"].dt.year
    trend_df["MONTH"] = trend_df["TXN_DATE"].dt.month

    # =================================================
    # YEARLY PIE (ALTair Donut)
    # =================================================
    st.subheader("📅 Year-wise High Risk Distribution")

    yearly_df = trend_df.groupby("YEAR")["HIGH_RISK_TXNS"].sum().reset_index()

    yearly_pie = alt.Chart(yearly_df).mark_arc(innerRadius=70).encode(
        theta="HIGH_RISK_TXNS:Q",
        color="YEAR:N"
    )

    st.altair_chart(yearly_pie, use_container_width=True)

    # =================================================
    # MONTHLY PIE
    # =================================================
    st.subheader("🗓 Monthly High Risk Distribution")

    year_m = st.selectbox(
        "Select Year",
        sorted(trend_df["YEAR"].unique()),
        key="month_year_filter"
    )

    monthly_df = trend_df[trend_df["YEAR"] == year_m] \
        .groupby("MONTH")["HIGH_RISK_TXNS"].sum().reset_index()

    monthly_pie = alt.Chart(monthly_df).mark_arc(innerRadius=70).encode(
        theta="HIGH_RISK_TXNS:Q",
        color="MONTH:O"
    )

    st.altair_chart(monthly_pie, use_container_width=True)

    # =================================================
    # DAILY TREND
    # =================================================
    st.subheader("📆 Daily High Risk Trend")

    c1, c2 = st.columns(2)

    with c1:
        year_d = st.selectbox(
            "Year",
            sorted(trend_df["YEAR"].unique()),
            key="daily_year"
        )

    with c2:
        month_d = st.selectbox(
            "Month",
            sorted(trend_df[trend_df["YEAR"] == year_d]["MONTH"].unique()),
            key="daily_month"
        )

    daily_df = trend_df[
        (trend_df["YEAR"] == year_d) &
        (trend_df["MONTH"] == month_d)
    ]

    daily_chart = alt.Chart(daily_df).mark_line(point=True).encode(
        x="TXN_DATE:T",
        y="HIGH_RISK_TXNS:Q"
    )

    st.altair_chart(daily_chart, use_container_width=True)

st.divider()

# =====================================================
# LOCATION RISK
# =====================================================
st.subheader("🌍 High Risk by Location")

loc_df = run_query("""
SELECT *
FROM FIN_FRAUD_DB.GOLD.VW_LOCATION_RISK
""")

if not loc_df.empty:

    loc_chart = alt.Chart(loc_df).mark_bar().encode(
        x="LOCATION:N",
        y="HIGH_RISK_TXNS:Q"
    )

    st.altair_chart(loc_chart, use_container_width=True)

st.divider()

# =====================================================
# DRILL DOWN
# =====================================================
st.subheader("🎯 Transaction Drill Down")

dash_df = run_query("""
SELECT *
FROM FIN_FRAUD_DB.GOLD.VW_FRAUD_DASHBOARD
LIMIT 2000
""")

if not dash_df.empty:

    dash_df["TXN_TIME"] = pd.to_datetime(dash_df["TXN_TIME"])
    dash_df["YEAR"] = dash_df["TXN_TIME"].dt.year
    dash_df["MONTH"] = dash_df["TXN_TIME"].dt.month

    f1, f2, f3, f4 = st.columns(4)

    with f1:
        year = st.selectbox("Year", sorted(dash_df["YEAR"].unique()))

    with f2:
        month = st.selectbox("Month", sorted(dash_df["MONTH"].unique()))

    with f3:
        city = st.selectbox(
            "City",
            ["ALL"] + sorted(dash_df["LOCATION"].dropna().unique())
        )

    with f4:
        risk = st.selectbox(
            "Risk",
            ["ALL", "HIGH_RISK", "MEDIUM_RISK", "LOW_RISK"]
        )

    filtered_df = dash_df[
        (dash_df["YEAR"] == year) &
        (dash_df["MONTH"] == month)
    ]

    if city != "ALL":
        filtered_df = filtered_df[filtered_df["LOCATION"] == city]

    if risk != "ALL":
        filtered_df = filtered_df[
            filtered_df["FRAUD_RISK_LEVEL"] == risk
        ]

    st.dataframe(filtered_df, use_container_width=True)

st.success("✅ Dashboard Loaded Successfully")
