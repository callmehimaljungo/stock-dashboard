import streamlit as st
import pandas as pd
import math
from pathlib import Path
import boto3
import io
import pyarrow.parquet as pq
import plotly.graph_objects as go

# 1. Hàm tải và đọc Parquet từ R2
# -----------------------------
@st.cache_data
def list_versions():
    aws = st.secrets["aws"]
    sess = boto3.session.Session(
        aws_access_key_id=aws["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=aws["AWS_SECRET_ACCESS_KEY"],
        region_name="auto"
    )
    s3 = sess.client("s3", endpoint_url=aws["ENDPOINT_URL"])

    prefix = "aapl_versions_export/"
    bucket = aws["BUCKET"]
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = [obj["Key"] for obj in resp.get("Contents", []) if obj["Key"].endswith(".parquet")]
    return sorted(files)

@st.cache_data
def read_parquet_from_r2(key):
    aws = st.secrets["aws"]
    s3 = boto3.client("s3",
        aws_access_key_id=aws["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=aws["AWS_SECRET_ACCESS_KEY"],
        endpoint_url=aws["ENDPOINT_URL"],
    )
    obj = s3.get_object(Bucket=aws["BUCKET"], Key=key)
    buf = io.BytesIO(obj["Body"].read())
    df = pq.read_table(buf).to_pandas()
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df

# -------------------------------
# 2. Giao diện chọn version
# -------------------------------

st.title("📈 AAPL Version Browser from Delta Export")

# Lấy danh sách file .parquet từ Cloudflare R2
all_versions = list_versions()

# Khởi tạo mapping label → path file part đầu tiên trong từng version
version_map = {
    "1 năm (1980)": None,
    "5 năm (1980–1984)": None,
    "10 năm (1980–1989)": None,
    "20 năm (1980–1999)": None,
    "Toàn bộ (1980–2024)": None,
}

# Mapping label → prefix thư mục
version_prefixes = {
    "1 năm (1980)": "aapl_versions_export/ver_0.parquet/",
    "5 năm (1980–1984)": "aapl_versions_export/ver_1.parquet/",
    "10 năm (1980–1989)": "aapl_versions_export/ver_2.parquet/",
    "20 năm (1980–1999)": "aapl_versions_export/ver_3.parquet/",
    "Toàn bộ (1980–2024)": "aapl_versions_export/ver_4.parquet/",
}

# Duyệt từng version, lấy part file đầu tiên khớp prefix
for label, prefix in version_prefixes.items():
    part_files = [v for v in all_versions if v.startswith(prefix) and v.endswith(".parquet")]
    if part_files:
        version_map[label] = part_files[0]

# Dropdown UI
label = st.selectbox("🕓 Chọn giai đoạn dữ liệu", list(version_map.keys()))

# Đường dẫn thật để tải file
selected_version = version_map[label]

# -------------------------------
# 3. Đọc và hiển thị dữ liệu
# -------------------------------

df = read_parquet_from_r2(selected_version)
df.columns = [c.lower() for c in df.columns]
df["close"] = df["close"] * 2


st.subheader(f"📄 Xem trước dữ liệu: {label}")
st.caption(f"📂 Bảng dữ liệu chi tiết: ")
st.dataframe(df.head(100), use_container_width=True)

# -------------------------------
# 4. Biểu đồ nến (Candlestick)
# -------------------------------
import numpy as np

# Tính SMA
df["sma5"] = df["close"].rolling(5).mean()
df["sma10"] = df["close"].rolling(10).mean()

# Tính daily return
df["daily_return"] = df["close"].pct_change()

# Xác định màu nến: xanh nếu tăng, đỏ nếu giảm
df["color"] = np.where(df["close"] > df["open"], "lime", "red")

# Tạo biểu đồ Figure
fig = go.Figure()

# Vẽ thân nến: open → close (2 đầu)
for i in range(len(df)):
    fig.add_trace(go.Scatter(
        x=[df["date"].iloc[i], df["date"].iloc[i]],
        y=[df["open"].iloc[i], df["close"].iloc[i]],
        mode="lines",
        line=dict(color=df["color"].iloc[i], width=4),
        showlegend=False
    ))

# Vẽ SMA5 và SMA10
fig.add_trace(go.Scatter(
    x=df["date"],
    y=df["sma5"],
    mode="lines",
    line=dict(color="orange", width=1),
    name="SMA 5"
))
fig.add_trace(go.Scatter(
    x=df["date"],
    y=df["sma10"],
    mode="lines",
    line=dict(color="blue", width=1),
    name="SMA 10"
))

# Cập nhật layout
fig.update_layout(
    title=f"📈 Biến đồ biến động giá trong khoảng thời gian – {label}",
    xaxis_title="Ngày",
    yaxis_title="Giá",
    template="plotly_dark"
)

# Hiển thị
st.plotly_chart(fig, use_container_width=True)

