import streamlit as st
import pandas as pd
import s3fs
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import time

# Configuration
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "ecommerce-raw"
DATA_PATH = f"{BUCKET_NAME}/data/clicks"

@st.cache_resource
def get_filesystem():
    """Creates an S3 filesystem connection to MinIO."""
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={'endpoint_url': MINIO_ENDPOINT},
        use_listings_cache=False
    )

def load_recent_data():
    """Fetches Parquet files from MinIO and merges them into a DataFrame."""
    fs = get_filesystem()
    
    try:
        # Find and load all parquet files
        files = fs.glob(f"s3://{DATA_PATH}/**/*.parquet")
        
        if not files:
            return pd.DataFrame()
        
        dfs = []
        for file in files:
            with fs.open(file) as f:
                dfs.append(pd.read_parquet(f))
        
        if not dfs:
            return pd.DataFrame()
            
        df = pd.concat(dfs, ignore_index=True)
        
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
        return df
        
    except Exception as e:
        st.error(f"MinIO connection error: {e}")
        return pd.DataFrame()

# Dashboard Layout
st.set_page_config(page_title="Real-Time E-commerce Lake", layout="wide")

st.title("E-commerce Live Stream (Data Lake)")
st.markdown(f"Data fetched directly from **MinIO S3** (Bucket: `{BUCKET_NAME}`)")

if st.button('Refresh Data'):
    st.rerun()

df = load_recent_data()

if df.empty:
    st.warning("Waiting for data in Data Lake... (Ensure Spark and Generator are running)")
else:
    # KPI Row
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    
    total_events = len(df)
    revenue = df[df['event_type'] == 'purchase']['price'].sum()
    
    unique_users = df['user_id'].nunique()
    purchasers = df[df['event_type'] == 'purchase']['user_id'].nunique()
    conversion = (purchasers / unique_users * 100) if unique_users > 0 else 0
    
    top_prod = df[df['event_type'] == 'purchase']['product_name'].mode()
    top_prod_name = top_prod[0] if not top_prod.empty else "None"

    kpi1.metric("Events in Lake", total_events)
    kpi2.metric("Revenue (PLN)", f"{revenue:,.2f} PLN")
    kpi3.metric("Conversion Rate", f"{conversion:.2f}%")
    kpi4.metric("Bestseller", top_prod_name)

    st.divider()

    # Charts Row 1
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Traffic vs Purchases")
        df_time = df.set_index('timestamp').resample('1T')['event_type'].count().reset_index()
        fig_time = px.area(df, x='timestamp', y='price', color='event_type', 
                           title="Activity over Time (Price/Value)", markers=True)
        st.plotly_chart(fig_time, use_container_width=True)

    with col2:
        st.subheader("Traffic Heatmap (Cities)")
        city_counts = df['city'].value_counts().head(10).reset_index()
        city_counts.columns = ['City', 'Traffic']
        fig_city = px.bar(city_counts, x='Traffic', y='City', orientation='h', color='Traffic',
                          color_continuous_scale='Viridis')
        st.plotly_chart(fig_city, use_container_width=True)

    # Charts Row 2
    col3, col4 = st.columns(2)
    
    with col3:
        st.subheader("Devices and OS")
        fig_sun = px.sunburst(df, path=['device_type', 'os'], values='price', 
                              title="Cart Value by Technology")
        st.plotly_chart(fig_sun, use_container_width=True)
        
    with col4:
        st.subheader("Traffic Sources")
        traffic = df['traffic_source'].value_counts().reset_index()
        traffic.columns = ['Source', 'Count']
        fig_pie = px.pie(traffic, names='Source', values='Count', hole=0.4)
        st.plotly_chart(fig_pie, use_container_width=True)

    # Raw Data Preview
    with st.expander("Raw Data Preview (JSON -> Parquet)"):
        st.dataframe(df.sort_values(by='timestamp', ascending=False).head(50))