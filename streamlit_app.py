import streamlit as st
import pandas as pd
import math
from pathlib import Path
import boto3
import io
import pyarrow.parquet as pq
import plotly.graph_objects as go

@st.cache_data
def get_gdp_data():
    aws = st.secrets["aws"]
    sess = boto3.session.Session(
        aws_access_key_id=aws["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=aws["AWS_SECRET_ACCESS_KEY"],
        region_name="auto",
    )
    s3 = sess.client("s3", endpoint_url=aws["ENDPOINT_URL"])

    # 1) List các file trong thư mục fx_daily.parquet/
    resp = s3.list_objects_v2(Bucket=aws["BUCKET"], Prefix="fx_daily.parquet/")
    parts = [obj["Key"] for obj in resp.get("Contents", []) if obj["Key"].endswith(".parquet")]
    if not parts:
        raise FileNotFoundError("Không tìm thấy file .parquet trong fx_daily.parquet/")
    key = parts[0]  # thường chỉ có 1 part

    # 2) Lấy nội dung file
    obj = s3.get_object(Bucket=aws["BUCKET"], Key=key)
    buf = io.BytesIO(obj["Body"].read())

    # 3) Đọc Parquet về pandas
    df = pq.read_table(buf).to_pandas()
    df["date"] = pd.to_datetime(df["date"])
    return df


# 1. Load data
df = get_gdp_data()

# 2. Tiêu đề
st.title("📈 FX Daily Dashboard")

# 3. Hiển thị bảng
st.subheader("Bảng dữ liệu FX")
st.dataframe(df, use_container_width=True)

# 4. Vẽ biểu đồ line chart cột close
# Chuẩn bị figure candlestick
fig = go.Figure(
    data=[
        go.Candlestick(
            x=df["date"],
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
            name="OHLC",
        )
    ]
)

# Tùy chỉnh layout
fig.update_layout(
    title="📊 Biểu đồ nến EUR/USD",
    xaxis_title="Ngày",
    yaxis_title="Giá",
    xaxis_rangeslider_visible=False,      # ẩn thanh range-slider (tuỳ chọn)
    template="plotly_dark",               # có thể chọn "plotly", "ggplot2", ...
)

# Hiển thị trong Streamlit
st.subheader("Biểu đồ nến (Candlestick)")
st.plotly_chart(fig, use_container_width=True)
