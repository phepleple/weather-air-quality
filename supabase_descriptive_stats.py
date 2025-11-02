#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
supabase_descriptive_stats.py
- Kết nối Supabase PostgreSQL
- Đọc weatherdata & airqualitydata (lọc theo ngày + city)
- Ghép theo city_id + timestamp(làm tròn giờ)
- Tính descriptive stats: mean, median, mode, std
- Xuất bảng CSV/Excel + vẽ biểu đồ (matplotlib)

Cách chạy (Windows PowerShell/VS Code Terminal):
  python supabase_descriptive_stats.py --days-back 45 --cities "1,2" --save-excel

Yêu cầu:
  pip install pandas matplotlib psycopg2-binary numpy openpyxl
"""

import argparse
from pathlib import Path
import numpy as np
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt

# 🔐 DÁN CONNECTION_URL ĐÃ HOẠT ĐỘNG CỦA BẠN Ở ĐÂY:
# Ví dụ: "postgresql://postgres:%40ADY201...@db.xxx.supabase.co:5432/postgres"
CONNECTION_URL = "postgresql://postgres:Weather_air_quality@db.zbpzgdujjaxhszhjaoiv.supabase.co:5432/postgres"

CITY_MAP = {1: "Hanoi", 2: "Danang"}  # cập nhật nếu bạn dùng thêm city_id khác

def parse_args():
    p = argparse.ArgumentParser(description="Compute descriptive stats & plots from Supabase data")
    p.add_argument("--days-back", type=int, default=45, help="Số ngày gần nhất (mặc định 45)")
    p.add_argument("--cities", default="1,2", help='Danh sách city_id, ví dụ "1,2"')
    p.add_argument("--outdir", default="figures", help="Thư mục lưu biểu đồ")
    p.add_argument("--save-csv", action="store_true", help="Lưu bảng thống kê ra CSV")
    p.add_argument("--save-excel", action="store_true", help="Lưu bảng thống kê ra Excel")
    return p.parse_args()

# ---------- DB & READ ----------
def connect():
    return psycopg2.connect(CONNECTION_URL, sslmode="require")

def read_weather(conn, days_back, city_ids):
    sql = """
    select
      city_id,
      date_trunc('hour', ts) as timestamp,
      temp as temperature,
      humidity,
      wind_speed
    from public.weatherdata
    where city_id = any(%s)
      and ts >= (now() at time zone 'UTC') - interval %s
    order by ts;
    """
    interval = f"'{days_back} days'"
    return pd.read_sql(sql, conn, params=(city_ids, interval))


def read_air(conn, days_back, city_ids):
    sql = """
    select
      city_id,
      date_trunc('hour', ts) as timestamp,
      aqi,
      pm2_5,
      pm10,
      co, no, no2, o3, so2
    from public.airqualitydata
    where city_id = any(%s)
      and ts >= (now() at time zone 'UTC') - interval %s
    order by ts;
    """
    interval = f"'{days_back} days'"
    return pd.read_sql(sql, conn, params=(city_ids, interval))


def merge_hourly(df_w, df_a):
    df = pd.merge(df_w, df_a, on=["city_id", "timestamp"], how="outer")
    df = df.sort_values(["city_id", "timestamp"]).reset_index(drop=True)
    df["city_name"] = df["city_id"].map(CITY_MAP)
    return df

# ---------- STATS ----------
def safe_mode(s: pd.Series):
    # Trả 1 giá trị mode; nếu rỗng trả NaN
    m = s.mode(dropna=True)
    return m.iloc[0] if len(m) else np.nan

def compute_stats(df: pd.DataFrame):
    """
    Trả về bảng tidy có cột: city_id, city_name, variable, count, mean, median, mode, std, min, max
    """
    numeric_cols = [c for c in [
        "temperature", "humidity", "wind_speed", "aqi", "pm2_5", "pm10",
        "co", "no", "no2", "o3", "so2"
    ] if c in df.columns]

    rows = []
    for cid, sub in df.groupby("city_id"):
        cname = CITY_MAP.get(cid, str(cid))
        for col in numeric_cols:
            x = pd.to_numeric(sub[col], errors="coerce")
            if x.notna().sum() == 0:
                continue
            rows.append({
                "city_id": cid,
                "city_name": cname,
                "variable": col,
                "count": int(x.notna().sum()),
                "mean": float(x.mean()),
                "median": float(x.median()),
                "mode": float(safe_mode(x)),
                "std": float(x.std(ddof=1)),
                "min": float(x.min()),
                "max": float(x.max())
            })
    stats = pd.DataFrame(rows)
    stats = stats.sort_values(["variable", "city_id"]).reset_index(drop=True)
    return stats

# ---------- PLOTS ----------
def ensure_outdir(outdir):
    Path(outdir).mkdir(parents=True, exist_ok=True)

def plot_line(df, col, outdir):
    if col not in df.columns: return
    fig = plt.figure(figsize=(10,4))
    for cid, sub in df[df[col].notna()].groupby("city_id"):
        plt.plot(sub["timestamp"], sub[col], label=CITY_MAP.get(cid, cid))
    plt.title(f"{col} over time")
    plt.xlabel("Time"); plt.ylabel(col); plt.legend(); plt.xticks(rotation=20)
    fig.savefig(Path(outdir)/f"line_{col}.png", bbox_inches="tight", dpi=150); plt.close(fig)

def plot_box_by_city(df, col, outdir):
    if col not in df.columns: return
    data, labels = [], []
    for cid, sub in df.groupby("city_id"):
        v = pd.to_numeric(sub[col], errors="coerce").dropna().values
        if len(v):
            data.append(v); labels.append(CITY_MAP.get(cid, cid))
    if not data: return
    fig = plt.figure(figsize=(6,4))
    plt.boxplot(data, labels=labels)
    plt.title(f"Distribution of {col} by city")
    plt.ylabel(col)
    fig.savefig(Path(outdir)/f"box_{col}_by_city.png", bbox_inches="tight", dpi=150); plt.close(fig)

def plot_hist_with_stats(df, col, outdir):
    if col not in df.columns: return
    # 2 histogram chồng nhau: HN và ĐN; vẽ đường mean/median
    fig = plt.figure(figsize=(8,4))
    bins = 30
    for cid, color in [(1, None), (2, None)]:  # không set màu cụ thể (theo yêu cầu chung)
        sub = pd.to_numeric(df[df["city_id"]==cid][col], errors="coerce").dropna()
        if len(sub)==0: continue
        plt.hist(sub, bins=bins, alpha=0.5, label=f"{CITY_MAP.get(cid, cid)}")
        m = sub.mean(); med = sub.median()
        plt.axvline(m, linestyle="--")
        plt.axvline(med, linestyle=":")
    plt.title(f"Histogram of {col} (mean --, median :)")
    plt.xlabel(col); plt.ylabel("Frequency"); plt.legend()
    fig.savefig(Path(outdir)/f"hist_{col}_with_stats.png", bbox_inches="tight", dpi=150); plt.close(fig)

def plot_corr_heatmap(df, outdir):
    num_cols = df.select_dtypes(include=[np.number]).columns
    if len(num_cols) < 2: return
    corr = df[num_cols].corr()
    fig = plt.figure(figsize=(6,5))
    plt.imshow(corr, aspect="auto")
    plt.title("Correlation heatmap (numeric)")
    plt.colorbar()
    plt.xticks(range(len(num_cols)), num_cols, rotation=45, ha="right")
    plt.yticks(range(len(num_cols)), num_cols)
    fig.savefig(Path(outdir)/"heatmap_corr.png", bbox_inches="tight", dpi=150); plt.close(fig)

def main():
    args = parse_args()

    # Parse cities "1,2" -> [1,2]
    city_ids = []
    for x in args.cities.split(","):
        x = x.strip()
        if x:
            try: city_ids.append(int(x))
            except: pass
    if not city_ids: city_ids = [1,2]

    print("🔌 Connecting to Supabase ...")
    conn = connect()
    print("✅ Connected.\n")

    print(f"⬇️ Reading {args.days_back} recent days for city_id={city_ids} ...")
    df_w = read_weather(conn, args.days_back, city_ids)
    df_a = read_air(conn, args.days_back, city_ids)
    print(f"- weather rows: {len(df_w)}")
    print(f"- air rows    : {len(df_a)}")

    df = merge_hourly(df_w, df_a)
    print(f"- merged rows : {len(df)}")

    # Compute stats
    stats = compute_stats(df)
    print("\n📊 Descriptive stats (head):")
    print(stats.head(12))

    # Save tables
    outdir = Path(args.outdir)
    ensure_outdir(outdir)
    if args.save_csv:
        stats.to_csv("descriptive_stats.csv", index=False)
        df.to_csv("merged_data_preview.csv", index=False)
        print("💾 Saved: descriptive_stats.csv, merged_data_preview.csv")
    if args.save_excel:
        with pd.ExcelWriter("descriptive_stats.xlsx", engine="openpyxl") as w:
            stats.to_excel(w, sheet_name="stats", index=False)
            df.head(5000).to_excel(w, sheet_name="sample", index=False)
        print("💾 Saved: descriptive_stats.xlsx")

    # Plots
    print("\n🖼  Generating charts ...")
    # Line charts
    for col in ["temperature","aqi"]:
        plot_line(df, col, outdir)

    # Boxplots by city
    for col in ["aqi","temperature","humidity"]:
        plot_box_by_city(df, col, outdir)

    # Histograms with mean/median lines
    for col in ["aqi","temperature"]:
        plot_hist_with_stats(df, col, outdir)

    # Correlation heatmap (all numeric)
    plot_corr_heatmap(df, outdir)

    print(f"✅ Done. Charts saved in: {outdir.resolve()}")

    conn.close()
    print("🔒 Closed connection.")

if __name__ == "__main__":
    main()
