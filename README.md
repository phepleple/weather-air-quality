import os
import requests
import psycopg2
from datetime import datetime
import pytz
from dotenv import load_dotenv

# 📂 1️⃣ Nạp biến môi trường (.env hoặc GitHub Secrets)
load_dotenv()

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

if not SUPABASE_DB_URL or not OPENWEATHER_API_KEY:
    raise ValueError("❌ Lỗi: Thiếu SUPABASE_DB_URL hoặc OPENWEATHER_API_KEY. Kiểm tra .env hoặc Secrets.")

# 🏙️ 2️⃣ Danh sách thành phố cần thu thập
CITIES = {
    "Hanoi": {"lat": 21.0285, "lon": 105.8542},
    "Danang": {"lat": 16.0544, "lon": 108.2022}
}

# 🕒 3️⃣ Hàm lấy thời gian hiện tại theo giờ Việt Nam
def vn_time():
    tz = pytz.timezone("Asia/Ho_Chi_Minh")
    now_vn = datetime.now(tz).replace(second=0, microsecond=0)
    return now_vn.replace(tzinfo=None)  # loại bỏ tzinfo để PostgreSQL lưu đúng


# ☁️ 4️⃣ Hàm lấy dữ liệu thời tiết
def get_weather(lat, lon):
    url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=metric"
    r = requests.get(url)
    r.raise_for_status()  # 🚨 Thêm kiểm tra lỗi API
    data = r.json()
    return {
        "temp": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "weather": data["weather"][0]["main"],
        "wind_speed": data["wind"]["speed"]
    }

# 🌫️ 5️⃣ Hàm lấy dữ liệu chất lượng không khí
def get_air_quality(lat, lon):
    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}"
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()["list"][0]
    return {
        "aqi": data["main"]["aqi"],
        "co": data["components"]["co"],
        "no": data["components"]["no"],
        "no2": data["components"]["no2"],
        "o3": data["components"]["o3"],
        "so2": data["components"]["so2"],
        "pm2_5": data["components"]["pm2_5"],
        "pm10": data["components"]["pm10"]
    }

# 🗄️ 6️⃣ Hàm ghi dữ liệu vào Supabase
def insert_data(city, weather, air):
    try:
        # ✅ Không để sslmode=require để tránh lỗi "invalid sslmode"
        conn = psycopg2.connect(SUPABASE_DB_URL)
        cur = conn.cursor()
        ts = vn_time()

        # WeatherData
        cur.execute("""
            INSERT INTO WeatherData (city_id, ts, temp, humidity, weather, wind_speed)
            SELECT city_id, %s, %s, %s, %s, %s FROM Cities WHERE city_name = %s
            ON CONFLICT (city_id, ts) DO NOTHING;
        """, (ts, weather["temp"], weather["humidity"], weather["weather"], weather["wind_speed"], city))

        # AirQualityData
        cur.execute("""
            INSERT INTO AirQualityData (city_id, ts, aqi, co, no, no2, o3, so2, pm2_5, pm10)
            SELECT city_id, %s, %s, %s, %s, %s, %s, %s, %s, %s FROM Cities WHERE city_name = %s
            ON CONFLICT (city_id, ts) DO NOTHING;
        """, (
            ts, air["aqi"], air["co"], air["no"], air["no2"], air["o3"], air["so2"], air["pm2_5"], air["pm10"], city
        ))

        conn.commit()
        print(f"✅ Đã insert dữ liệu cho {city} lúc {ts}")

    except psycopg2.OperationalError as e:
        print(f"❌ Lỗi kết nối database: {e}")
    except Exception as e:
        print(f"❌ Lỗi khi insert dữ liệu: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

# 🚀 7️⃣ Main
if __name__ == "__main__":
    for city, coords in CITIES.items():
        print(f"🚀 Bắt đầu thu thập dữ liệu cho {city}")
        try:
            weather = get_weather(coords["lat"], coords["lon"])
            air = get_air_quality(coords["lat"], coords["lon"])
            print("🌤 Weather:", weather)
            print("🌫 Air:", air)
            insert_data(city, weather, air)
        except Exception as e:
            print(f"⚠️ Bỏ qua {city} do lỗi: {e}")
