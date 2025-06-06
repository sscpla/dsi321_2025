import streamlit as st
import pandas as pd
import numpy as np
import os
import time
from datetime import timezone, datetime
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import altair as alt

# ---------------------- Configuration ----------------------
REFRESH_INTERVAL_DEFAULT = 30
ANOMALY_SENSITIVITY_DEFAULT = 10
MAX_DISPLAY_POINTS = 100
TIMESTAMP_COLUMN = 'scrape_timestamp_utc'
DISPLAY_TIME_COLUMN = 'display_time'
POWER_COLUMN = 'current_value_MW'
TEMP_COLUMN = 'temperature_C'

ACCESS_KEY = os.getenv("LAKEFS_ACCESS_KEY_ID", "access_key")
SECRET_KEY = os.getenv("LAKEFS_SECRET_ACCESS_KEY", "secret_key")
LAKEFS_ENDPOINT = os.getenv("LAKEFS_ENDPOINT_URL", "http://localhost:8001/")
REPO_NAME = "dataset"
BRANCH_NAME = "main"
TARGET_PARQUET_FILE_PATH = "egat_datascraping/egat_realtime_power_history.parquet"
lakefs_s3_path = f"s3a://{REPO_NAME}/{BRANCH_NAME}/{TARGET_PARQUET_FILE_PATH}"
storage_options = {
    "key": ACCESS_KEY,
    "secret": SECRET_KEY,
    "client_kwargs": {
        "endpoint_url": LAKEFS_ENDPOINT
    }
}

st.set_page_config(page_title="EGAT Realtime Power Dashboard (lakeFS)", layout="wide")

# ---------------------- Helper Functions ----------------------
def detect_anomalies(data_series, contamination_factor=0.1):
    if data_series is None or data_series.empty or data_series.isnull().all():
        return np.array([False] * (len(data_series) if data_series is not None else 0))
    valid_data = data_series.dropna()
    if len(valid_data) < 2:
        return np.array([False] * len(data_series))
    scaled_data = StandardScaler().fit_transform(valid_data.values.reshape(-1, 1))
    predictions = IsolationForest(contamination=contamination_factor, random_state=42).fit_predict(scaled_data) == -1
    anomalies = np.array([False] * len(data_series))
    for i, index in enumerate(valid_data.index):
        anomalies[data_series.index.get_loc(index)] = predictions[i]
    return anomalies

@st.cache_data(ttl=REFRESH_INTERVAL_DEFAULT)
def load_data_from_lakefs(s3_path, storage_opts):
    df = pd.read_parquet(s3_path, storage_options=storage_opts)
    if pd.api.types.is_numeric_dtype(df[TIMESTAMP_COLUMN]):
        df[TIMESTAMP_COLUMN] = pd.to_datetime(df[TIMESTAMP_COLUMN], unit='us', errors='coerce')
    else:
        df[TIMESTAMP_COLUMN] = pd.to_datetime(df[TIMESTAMP_COLUMN], errors='coerce')
    df.dropna(subset=[TIMESTAMP_COLUMN], inplace=True)
    return df.sort_values(by=TIMESTAMP_COLUMN, ascending=False)

# ---------------------- UI Components ----------------------
def create_top_controls():
    with st.container():
        col1, col2, col3 = st.columns([1, 2, 2])
        with col1:
            auto_refresh = st.checkbox('Enable Auto-refresh', value=True)
        with col2:
            refresh_interval = st.slider('Refresh Interval (s)', 5, 120, REFRESH_INTERVAL_DEFAULT)
        with col3:
            contamination_factor = st.slider('Anomaly Sensitivity (%)', 1, 25, ANOMALY_SENSITIVITY_DEFAULT) / 100

        st.caption(f"UTC: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    return auto_refresh, refresh_interval, contamination_factor

def display_metrics(latest_data_row, anomaly_status):
    col1, col2, col3, col4 = st.columns(4)
    power = latest_data_row.get(POWER_COLUMN, "N/A")
    temp = latest_data_row.get(TEMP_COLUMN, "N/A")
    disp_time = latest_data_row.get(DISPLAY_TIME_COLUMN, "N/A")
    col1.metric("Power Output (MW)", f"{power:,.1f}" if pd.notna(power) else "N/A")
    col2.metric("Temperature (°C)", f"{temp:.1f}" if pd.notna(temp) else "N/A")
    col3.metric("Source Time", str(disp_time))
    col4.markdown(f"<span style='color:{'red' if anomaly_status else 'green'};'>{'Anomaly ⚠️' if anomaly_status else 'Normal ✅'}</span>", unsafe_allow_html=True)

def display_charts(chart_data):
    chart_col1, chart_col2 = st.columns(2)
    with chart_col1:
        st.subheader(f"{POWER_COLUMN}")
        line = alt.Chart(chart_data).mark_line(color='steelblue').encode(
            x=TIMESTAMP_COLUMN,
            y=POWER_COLUMN
        ).properties(height=300)
        st.altair_chart(line, use_container_width=True)
    with chart_col2:
        st.subheader(f"{TEMP_COLUMN}")
        line = alt.Chart(chart_data).mark_line(color='orange').encode(
            x=TIMESTAMP_COLUMN,
            y=TEMP_COLUMN
        ).properties(height=300)
        st.altair_chart(line, use_container_width=True)

def display_statistics(anomalies, chart_data):
    st.subheader("Chart Data Statistics")
    cols = st.columns(4)
    total_anomalies = anomalies.sum()
    anomaly_rate = (total_anomalies / len(anomalies)) * 100 if len(anomalies) > 0 else 0
    avg_power = chart_data[POWER_COLUMN].mean()
    peak_power = chart_data[POWER_COLUMN].max()
    cols[0].metric("Anomalies", f"{int(total_anomalies)}")
    cols[1].metric("Anomaly Rate", f"{anomaly_rate:.1f}%")
    cols[2].metric("Avg Power", f"{avg_power:,.1f} MW")
    cols[3].metric("Peak Power", f"{peak_power:,.1f} MW")

def display_recent_data_table(df_all_data, anomalies):
    st.subheader("Recent Data (Latest 10)")
    df_display = df_all_data.head(10).copy()
    df_display['Status'] = ['Anomaly ⚠️' if anom else 'Normal ✅' for anom in anomalies[:len(df_display)]]
    cols = [TIMESTAMP_COLUMN, DISPLAY_TIME_COLUMN, POWER_COLUMN, TEMP_COLUMN, 'Status']
    st.dataframe(df_display[cols], hide_index=True)

# ---------------------- App Runner ----------------------
def run_app():
    st.markdown(
    """
    <style>
        html, body, [class*="css"] {
            color: #003366; /* สีน้ำเงินเข้ม */
            background-color: #f0f8ff; /* สีพื้นหลังฟ้าอ่อน */
        }
        .stApp {
            background-color: #f0f8ff;
        }
        .css-1v0mbdj edgvbvh3 {  /* ปรับ metric สีอักษร (อาจเปลี่ยนตามเวอร์ชัน Streamlit) */
            color: #003366;
        }
        .stMarkdown {
            color: #003366;
        }
        .stDataFrame {
            color: #003366;
        }
    </style>
    """,
    unsafe_allow_html=True)

    st.title("EGAT Realtime Power Generation Dashboard (via lakeFS)")
    auto_refresh, refresh_interval, contamination = create_top_controls()
    last_refresh_ph = st.empty()
    metrics_ph = st.container()
    table_ph = st.container()
    charts_stats_ph = st.container()

    df_all_data = load_data_from_lakefs(lakefs_s3_path, storage_options)
    latest_row = df_all_data.iloc[0] if not df_all_data.empty else {}
    chart_data = df_all_data.head(MAX_DISPLAY_POINTS).sort_values(TIMESTAMP_COLUMN, ascending=True)
    anomalies_chart = detect_anomalies(chart_data[POWER_COLUMN], contamination) if POWER_COLUMN in chart_data else np.array([])
    anomaly_latest = anomalies_chart[-1] if len(anomalies_chart) > 0 else False

    with metrics_ph:
        display_metrics(latest_row, anomaly_latest)
    with table_ph:
        display_recent_data_table(df_all_data, anomalies_chart)
    with charts_stats_ph:
        display_charts(chart_data)
        display_statistics(anomalies_chart, chart_data)

    last_refresh_ph.caption(f"Dashboard refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()

if __name__ == "__main__":
    run_app()
