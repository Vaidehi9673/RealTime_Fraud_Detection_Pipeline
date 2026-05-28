"""
app.py — Real-Time Transaction Anomaly Dashboard (Streamlit)
============================================================
Phase 5 of the Real-Time Transaction Anomaly Detection Engine.

This dashboard polls the DynamoDB `transaction_anomalies` table every
5 seconds and renders a live feed of flagged transactions.

Run it
------
    streamlit run app.py

What you see
------------
• KPI cards:  total anomalies detected | latest anomaly timestamp | most
              targeted user ID
• Live table: the 50 most recent anomaly records from DynamoDB
• Amount chart: bar chart of max_amount per anomaly window
• Flags breakdown: pie chart showing HIGH_AMOUNT vs HIGH_FREQUENCY splits
"""

import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

import boto3
import pandas as pd
import plotly.express as px
import streamlit as st
from boto3.dynamodb.conditions import Attr
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")
TABLE_NAME: str = os.getenv("DYNAMODB_ANOMALIES_TABLE", "transaction_anomalies")
REFRESH_INTERVAL: int = 5  # seconds between auto-refreshes
MAX_ITEMS: int = 50         # most recent anomalies to display

# ── Page setup ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Transaction Anomaly Monitor",
    page_icon="🚨",
    layout="wide",
)

st.title("🚨 Real-Time Transaction Anomaly Monitor")
st.caption(
    f"Auto-refreshes every {REFRESH_INTERVAL}s · "
    f"Source: DynamoDB `{TABLE_NAME}` · "
    f"Region: `{AWS_REGION}`"
)


# ── DynamoDB helpers ──────────────────────────────────────────────────────────

@st.cache_resource
def get_dynamodb_table():
    """Return a cached DynamoDB Table resource (one boto3 client per session)."""
    ddb = boto3.resource("dynamodb", region_name=AWS_REGION)
    return ddb.Table(TABLE_NAME)


def fetch_anomalies() -> List[Dict[str, Any]]:
    """
    Scan the anomalies table and return all records where is_anomaly=True.

    Note: a DynamoDB Scan reads every item — this is acceptable for a
    monitoring dashboard on a small table.  For a table with millions of rows,
    replace with a Query on a GSI (e.g. GSI on detected_at).
    """
    table = get_dynamodb_table()
    items: List[Dict[str, Any]] = []
    last_key = None

    # Paginate through all results (Scan returns max 1 MB per call)
    while True:
        kwargs: Dict[str, Any] = {
            "FilterExpression": Attr("is_anomaly").eq(True),
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key

        response = table.scan(**kwargs)
        items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break

    return items


def items_to_dataframe(items: List[Dict[str, Any]]) -> pd.DataFrame:
    """Convert DynamoDB items to a clean pandas DataFrame."""
    if not items:
        return pd.DataFrame(
            columns=[
                "user_id", "window_start", "window_end",
                "transaction_count", "max_amount", "anomaly_flags", "detected_at",
            ]
        )

    df = pd.DataFrame(items)

    # Ensure the columns we care about always exist
    for col_name in ["transaction_count", "max_amount"]:
        if col_name in df.columns:
            df[col_name] = pd.to_numeric(df[col_name], errors="coerce")

    for ts_col in ["window_start", "window_end", "detected_at"]:
        if ts_col in df.columns:
            df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)

    # Sort newest first
    if "detected_at" in df.columns:
        df = df.sort_values("detected_at", ascending=False)

    return df.head(MAX_ITEMS)


# ── Main render loop ──────────────────────────────────────────────────────────

placeholder = st.empty()

while True:
    with placeholder.container():
        # ── Fetch data ────────────────────────────────────────────────────────
        with st.spinner("Fetching latest anomalies from DynamoDB..."):
            try:
                raw_items = fetch_anomalies()
                df = items_to_dataframe(raw_items)
                fetch_error = None
            except Exception as exc:  # noqa: BLE001
                df = pd.DataFrame()
                fetch_error = str(exc)

        # ── Error state ───────────────────────────────────────────────────────
        if fetch_error:
            st.error(
                f"Could not connect to DynamoDB: `{fetch_error}`\n\n"
                "Make sure your AWS credentials are configured and "
                f"the `{TABLE_NAME}` table exists.\n\n"
                "Run `python infrastructure/setup_aws.py` to create it."
            )
            time.sleep(REFRESH_INTERVAL)
            continue

        # ── Empty state ───────────────────────────────────────────────────────
        if df.empty:
            st.info(
                "No anomalies detected yet. "
                "Start the generator (`python transaction_generator.py`) "
                "and the PySpark job (`bash run_spark_job.sh`) to see data here."
            )
            st.caption(
                f"Last checked: {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"
            )
            time.sleep(REFRESH_INTERVAL)
            continue

        # ── KPI cards ─────────────────────────────────────────────────────────
        kpi1, kpi2, kpi3 = st.columns(3)

        kpi1.metric(
            label="Total Anomalies Detected",
            value=len(df),
        )

        if "detected_at" in df.columns and not df["detected_at"].isna().all():
            latest_ts = df["detected_at"].max()
            kpi2.metric(
                label="Latest Anomaly",
                value=latest_ts.strftime("%H:%M:%S UTC") if pd.notna(latest_ts) else "—",
            )
        else:
            kpi2.metric(label="Latest Anomaly", value="—")

        if "user_id" in df.columns:
            top_user = df["user_id"].value_counts().idxmax()
            kpi3.metric(label="Most Flagged User", value=top_user)

        st.divider()

        # ── Charts (side by side) ─────────────────────────────────────────────
        col_left, col_right = st.columns(2)

        with col_left:
            st.subheader("Max Transaction Amount per Anomaly Window")
            if "max_amount" in df.columns and "window_start" in df.columns:
                chart_df = (
                    df[["window_start", "max_amount", "user_id"]]
                    .dropna(subset=["max_amount"])
                    .copy()
                )
                chart_df["window_start_str"] = chart_df["window_start"].dt.strftime(
                    "%H:%M"
                )
                fig_bar = px.bar(
                    chart_df.head(20),
                    x="window_start_str",
                    y="max_amount",
                    color="user_id",
                    labels={"window_start_str": "Window Start", "max_amount": "Max Amount ($)"},
                    title="",
                )
                fig_bar.update_layout(showlegend=False, margin=dict(t=10, b=10))
                st.plotly_chart(fig_bar, use_container_width=True)

        with col_right:
            st.subheader("Anomaly Flag Breakdown")
            if "anomaly_flags" in df.columns:
                all_flags = [
                    flag
                    for flags in df["anomaly_flags"].dropna()
                    for flag in (flags if isinstance(flags, list) else [flags])
                ]
                if all_flags:
                    flag_counts = pd.Series(all_flags).value_counts().reset_index()
                    flag_counts.columns = ["Flag", "Count"]
                    fig_pie = px.pie(
                        flag_counts,
                        names="Flag",
                        values="Count",
                        color_discrete_sequence=["#EF553B", "#636EFA"],
                        title="",
                    )
                    fig_pie.update_layout(margin=dict(t=10, b=10))
                    st.plotly_chart(fig_pie, use_container_width=True)
                else:
                    st.info("No flag data yet.")

        st.divider()

        # ── Live anomaly feed table ───────────────────────────────────────────
        st.subheader(f"Live Anomaly Feed (last {MAX_ITEMS})")

        display_cols = [
            c for c in [
                "detected_at", "user_id", "window_start", "window_end",
                "transaction_count", "max_amount", "anomaly_flags",
            ]
            if c in df.columns
        ]
        st.dataframe(
            df[display_cols],
            use_container_width=True,
            hide_index=True,
        )

        st.caption(
            f"Last refreshed: {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')} · "
            f"Showing {len(df)} of {len(raw_items)} total anomalies"
        )

    time.sleep(REFRESH_INTERVAL)
    placeholder.empty()
