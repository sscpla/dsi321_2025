import streamlit as st
import pandas as pd
import numpy as np
import os
import time
from datetime import timezone, datetime, timedelta
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
import requests
import matplotlib.pyplot as plt

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

REFRESH_INTERVAL_DEFAULT = 30
FORECAST_HORIZON_DEFAULT = 12  # Default prediction horizon (number of future points)
MAX_DISPLAY_POINTS = 100
TIMESTAMP_COLUMN = 'scrape_timestamp_utc'
DISPLAY_TIME_COLUMN = 'display_time'
POWER_COLUMN = 'current_value_MW'
TEMP_COLUMN = 'temperature_C'

st.set_page_config(page_title="EGAT Realtime Power Dashboard with Prediction", layout="wide")

def create_features(df, target_col):
    """Create time-based features for the model."""
    df = df.copy()
    df['hour'] = df[TIMESTAMP_COLUMN].dt.hour
    df['dayofweek'] = df[TIMESTAMP_COLUMN].dt.dayofweek
    df['month'] = df[TIMESTAMP_COLUMN].dt.month
    df['dayofyear'] = df[TIMESTAMP_COLUMN].dt.dayofyear
    
    # Add lag features
    df['lag_1'] = df[target_col].shift(1)
    df['lag_2'] = df[target_col].shift(2)
    df['lag_3'] = df[target_col].shift(3)
    
    # Add rolling mean features
    df['rolling_mean_3'] = df[target_col].rolling(window=3).mean()
    df['rolling_mean_6'] = df[target_col].rolling(window=6).mean()
    
    return df

def train_prediction_model(data, target_col, forecast_horizon=12):
    """Train a model to predict future values."""
    if data is None or data.empty or len(data) < 10:
        return None, None, []
    
    # Create features for model training
    df_model = create_features(data, target_col)
    df_model = df_model.dropna()
    
    if len(df_model) < 5:  # Need minimum data for reliable prediction
        return None, None, []
    
    # Define features for the model
    features = ['hour', 'dayofweek', 'month', 'dayofyear', 
                'lag_1', 'lag_2', 'lag_3', 
                'rolling_mean_3', 'rolling_mean_6']
    
    available_features = [f for f in features if f in df_model.columns]
    
    # Prepare training data
    X = df_model[available_features]
    y = df_model[target_col]
    
    # Train the model
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X, y)
    
    # Generate future timestamps
    last_timestamp = data[TIMESTAMP_COLUMN].max()
    future_timestamps = [last_timestamp + timedelta(minutes=30*i) for i in range(1, forecast_horizon+1)]
    
    # Create future feature matrix
    future_data = pd.DataFrame({TIMESTAMP_COLUMN: future_timestamps})
    future_data['hour'] = future_data[TIMESTAMP_COLUMN].dt.hour
    future_data['dayofweek'] = future_data[TIMESTAMP_COLUMN].dt.dayofweek
    future_data['month'] = future_data[TIMESTAMP_COLUMN].dt.month
    future_data['dayofyear'] = future_data[TIMESTAMP_COLUMN].dt.dayofyear
    
    # Add lag features based on the most recent data
    recent_values = data[target_col].iloc[:forecast_horizon].tolist()
    recent_values.reverse()  # Most recent first
    
    # Add lag values
    future_data['lag_1'] = np.nan
    future_data['lag_2'] = np.nan
    future_data['lag_3'] = np.nan
    future_data['rolling_mean_3'] = np.nan
    future_data['rolling_mean_6'] = np.nan
    
    # Make predictions one step at a time, updating lag features
    predictions = []
    
    for i in range(forecast_horizon):
        if i == 0:
            # For first prediction, use known values for lags
            future_data.loc[0, 'lag_1'] = data[target_col].iloc[0]
            future_data.loc[0, 'lag_2'] = data[target_col].iloc[1] if len(data) > 1 else data[target_col].iloc[0]
            future_data.loc[0, 'lag_3'] = data[target_col].iloc[2] if len(data) > 2 else data[target_col].iloc[0]
            future_data.loc[0, 'rolling_mean_3'] = data[target_col].iloc[:3].mean()
            future_data.loc[0, 'rolling_mean_6'] = data[target_col].iloc[:6].mean()
        else:
            # For subsequent predictions, use previous predictions for lags
            future_data.loc[i, 'lag_1'] = predictions[i-1]
            future_data.loc[i, 'lag_2'] = future_data.loc[i-1, 'lag_1'] if i > 1 else data[target_col].iloc[0]
            future_data.loc[i, 'lag_3'] = future_data.loc[i-1, 'lag_2'] if i > 2 else data[target_col].iloc[1] if len(data) > 1 else data[target_col].iloc[0]
            
            # Update rolling means
            recent_vals = [predictions[j] for j in range(max(0, i-3), i)]
            if len(recent_vals) < 3:
                recent_vals = list(data[target_col].iloc[:3-len(recent_vals)]) + recent_vals
            future_data.loc[i, 'rolling_mean_3'] = np.mean(recent_vals)
            
            recent_vals = [predictions[j] for j in range(max(0, i-6), i)]
            if len(recent_vals) < 6:
                recent_vals = list(data[target_col].iloc[:6-len(recent_vals)]) + recent_vals
            future_data.loc[i, 'rolling_mean_6'] = np.mean(recent_vals)
        
        # Make prediction for this timestamp
        pred = model.predict(future_data.loc[i:i, available_features])[0]
        predictions.append(pred)
    
    # Create forecast dataframe
    forecast_df = pd.DataFrame({
        TIMESTAMP_COLUMN: future_timestamps,
        target_col: predictions,
        'type': 'prediction'
    })
    
    return model, forecast_df, available_features

@st.cache_data(ttl=REFRESH_INTERVAL_DEFAULT)
def load_data_from_lakefs(s3_path, storage_opts):
    df = pd.read_parquet(s3_path, storage_options=storage_opts)
    if pd.api.types.is_numeric_dtype(df[TIMESTAMP_COLUMN]):
        df[TIMESTAMP_COLUMN] = pd.to_datetime(df[TIMESTAMP_COLUMN], unit='us', errors='coerce')
    else:
        df[TIMESTAMP_COLUMN] = pd.to_datetime(df[TIMESTAMP_COLUMN], errors='coerce')
    df.dropna(subset=[TIMESTAMP_COLUMN], inplace=True)
    return df.sort_values(by=TIMESTAMP_COLUMN, ascending=False)

def create_sidebar():
    auto_refresh = st.sidebar.checkbox('Enable Auto-refresh', value=True)
    refresh_interval = st.sidebar.slider('Refresh Interval (s)', 5, 120, REFRESH_INTERVAL_DEFAULT)
    forecast_horizon = st.sidebar.slider('Prediction Horizon (steps)', 1, 24, FORECAST_HORIZON_DEFAULT)
    st.sidebar.caption(f"UTC: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    return auto_refresh, refresh_interval, forecast_horizon

def display_metrics(latest_data_row, next_prediction):
    col1, col2, col3, col4 = st.columns(4)
    power = latest_data_row.get(POWER_COLUMN, "N/A")
    temp = latest_data_row.get(TEMP_COLUMN, "N/A")
    disp_time = latest_data_row.get(DISPLAY_TIME_COLUMN, "N/A")
    col1.metric("⚡ Current Power", f"{power:,.1f} MW" if pd.notna(power) else "N/A")
    col2.metric("🔮 Predicted Next", f"{next_prediction:,.1f} MW" if pd.notna(next_prediction) else "N/A", 
               delta=f"{next_prediction - power:,.1f}" if pd.notna(power) and pd.notna(next_prediction) else None)
    col3.metric("🌡️ Temperature", f"{temp:.1f}°C" if pd.notna(temp) else "N/A")
    col4.metric("🕒 Source Time", str(disp_time))

def display_charts(chart_data, prediction_data, target_col):
    chart_col1, chart_col2 = st.columns(2)
    
    with chart_col1:
        st.subheader(f"⚡ {target_col} with Predictions")
        
        # Combine historical and prediction data
        historical_data = chart_data[[TIMESTAMP_COLUMN, target_col]].copy()
        historical_data['type'] = 'historical'
        
        if prediction_data is not None and not prediction_data.empty:
            combined_data = pd.concat([historical_data, prediction_data[[TIMESTAMP_COLUMN, target_col, 'type']]])
        else:
            combined_data = historical_data
        
        # Plot with matplotlib for more control
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Plot historical data
        historical_subset = combined_data[combined_data['type'] == 'historical']
        ax.plot(historical_subset[TIMESTAMP_COLUMN], historical_subset[target_col], 
                label='Historical', color='blue', marker='o', markersize=3)
        
        # Plot prediction data if available
        prediction_subset = combined_data[combined_data['type'] == 'prediction']
        if not prediction_subset.empty:
            ax.plot(prediction_subset[TIMESTAMP_COLUMN], prediction_subset[target_col], 
                    label='Prediction', color='red', linestyle='--', marker='x', markersize=4)
            
            # Add vertical line to separate historical from prediction
            last_historical_time = historical_subset[TIMESTAMP_COLUMN].max()
            ax.axvline(x=last_historical_time, color='gray', linestyle='-', alpha=0.5)
        
        ax.set_xlabel('Time')
        ax.set_ylabel(target_col)
        ax.legend()
        ax.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        st.pyplot(fig)
    
    with chart_col2:
        st.subheader(f"🌡️ {TEMP_COLUMN}")
        st.line_chart(chart_data.set_index(TIMESTAMP_COLUMN)[TEMP_COLUMN], height=300)

def display_statistics(chart_data, prediction_data, target_col):
    st.subheader("📊 Prediction Statistics")
    cols = st.columns(4)
    
    current_power = chart_data[target_col].iloc[0] if not chart_data.empty else np.nan
    avg_power = chart_data[target_col].mean()
    
    if prediction_data is not None and not prediction_data.empty:
        predicted_avg = prediction_data[target_col].mean()
        max_predicted = prediction_data[target_col].max()
        min_predicted = prediction_data[target_col].min()
        
        # Calculate trend
        trend = "↗️ Increasing" if predicted_avg > current_power else "↘️ Decreasing" if predicted_avg < current_power else "➡️ Stable"
        
        cols[0].metric("Current Power", f"{current_power:,.1f} MW" if pd.notna(current_power) else "N/A")
        cols[1].metric("Avg Predicted", f"{predicted_avg:,.1f} MW", delta=f"{predicted_avg - current_power:,.1f}")
        cols[2].metric("Max Predicted", f"{max_predicted:,.1f} MW")
        cols[3].metric("Trend", trend)
    else:
        cols[0].metric("Current Power", f"{current_power:,.1f} MW" if pd.notna(current_power) else "N/A")
        cols[1].metric("Historical Avg", f"{avg_power:,.1f} MW")
        cols[2].metric("Prediction", "Not available")
        cols[3].metric("Trend", "N/A")

def display_prediction_table(prediction_data):
    st.subheader("🔮 Predictions")
    if prediction_data is not None and not prediction_data.empty:
        df_display = prediction_data.copy()
        df_display['Predicted Time'] = df_display[TIMESTAMP_COLUMN].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_display['Predicted Power (MW)'] = df_display[POWER_COLUMN].round(2)
        st.dataframe(df_display[['Predicted Time', 'Predicted Power (MW)']], hide_index=True)
    else:
        st.info("No predictions available. Ensure there's enough historical data for the model.")

def display_recent_data_table(df_all_data):
    st.subheader("📝 Recent Historical Data (Latest 10)")
    df_display = df_all_data.head(10).copy()
    cols = [TIMESTAMP_COLUMN, DISPLAY_TIME_COLUMN, POWER_COLUMN, TEMP_COLUMN]
    existing_cols = [c for c in cols if c in df_display.columns]
    st.dataframe(df_display[existing_cols], hide_index=True)

def run_app():
    st.title("⚡ EGAT Realtime Power Generation Dashboard with Prediction")
    auto_refresh, refresh_interval, forecast_horizon = create_sidebar()
    last_refresh_ph = st.empty()
    metrics_ph = st.container()
    charts_stats_ph = st.container()
    tables_ph = st.container()

    df_all_data = load_data_from_lakefs(lakefs_s3_path, storage_options)
    if df_all_data.empty:
        st.error("No data available. Please check your data source.")
        return
    
    # Sort for time-series analysis (oldest first for training)
    chart_data = df_all_data.head(MAX_DISPLAY_POINTS).sort_values(TIMESTAMP_COLUMN, ascending=True)
    
    # Train prediction model
    model, prediction_data, features_used = train_prediction_model(
        chart_data, POWER_COLUMN, forecast_horizon
    )
    
    latest_row = df_all_data.iloc[0] if not df_all_data.empty else {}
    next_prediction = prediction_data[POWER_COLUMN].iloc[0] if prediction_data is not None and not prediction_data.empty else np.nan

    with metrics_ph:
        display_metrics(latest_row, next_prediction)
    
    with charts_stats_ph:
        display_charts(chart_data, prediction_data, POWER_COLUMN)
        display_statistics(chart_data, prediction_data, POWER_COLUMN)
    
    with tables_ph:
        cols = st.columns(2)
        with cols[0]:
            display_prediction_table(prediction_data)
        with cols[1]:
            display_recent_data_table(df_all_data)
    
    # If model features were used, show them
    if features_used:
        with st.expander("Model Information"):
            st.write("Features used for prediction:", ", ".join(features_used))
            if model:
                feature_importance = pd.DataFrame({
                    'Feature': features_used,
                    'Importance': model.feature_importances_
                }).sort_values('Importance', ascending=False)
                st.bar_chart(feature_importance.set_index('Feature'))

    last_refresh_ph.caption(f"Dashboard refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()

if __name__ == "__main__":
    run_app()