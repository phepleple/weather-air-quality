import requests
import csv
import os
from datetime import datetime, timedelta

# 🔑 Lấy API Key từ biến môi trường (set trong GitHub Secrets)
API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    raise ValueError("❌ OPENWEATHER_API_KEY chưa được thiết lập trong môi trường!")

# 🌍 Thành phố và tọa độ
CITIES = {
    "Hanoi": {"lat": 21.0285, "lon": 105.8542},
    "Danang": {"lat": 16.0544, "lon": 108.2022},
}

# 📂 Tên file CSV (luôn tạo mới ở chế độ reset)
CSV_FILE = "weather_air_quality.csv"

# 📡 Hàm lấy dữ liệu thời tiết
def get_weather(lat, lon):
    try:
        url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"
        res = requests.get(url)
        res.raise_for_status()
        data = res.json()
        return {
            "temp": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "weather": data["weather"][0]["main"],
            "wind_speed": data["wind"]["speed"],
        }
    except Exception:
        return {"temp": "N/A", "humidity": "N/A", "weather": "N/A", "wind_speed": "N/A"}

# 📡 Hàm lấy dữ liệu chất lượng không khí
def get_air_quality(lat, lon):
    try:
        url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={API_KEY}"
        res = requests.get(url)
        res.raise_for_status()
        data = res.json()["list"][0]
        return {
            "aqi": data["main"]["aqi"],
            "co": data["components"]["co"],
            "no": data["components"]["no"],
            "no2": data["components"]["no2"],
            "o3": data["components"]["o3"],
            "so2": data["components"]["so2"],
            "pm2_5": data["components"]["pm2_5"],
            "pm10": data["components"]["pm10"],
        }
    except Exception:
        return {"aqi": "N/A", "co": "N/A", "no": "N/A", "no2": "N/A", "o3": "N/A",
                "so2": "N/A", "pm2_5": "N/A", "pm10": "N/A"}

# 📝 Crawl và lưu dữ liệu
def crawl_and_save():
    # Lấy thời gian hiện tại (UTC+7)
    timestamp = (datetime.utcnow() + timedelta(hours=7)).strftime("%Y-%m-%d %H:%M:%S")

    # Luôn tạo file mới với header
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "datetime", "city",
            "temp", "humidity", "weather", "wind_speed",
            "aqi", "co", "no", "no2", "o3", "so2", "pm2_5", "pm10"
        ])

        for city, coords in CITIES.items():
            weather = get_weather(coords["lat"], coords["lon"])
            air = get_air_quality(coords["lat"], coords["lon"])

            row = [
                timestamp, city,
                weather["temp"], weather["humidity"], weather["weather"], weather["wind_speed"],
                air["aqi"], air["co"], air["no"], air["no2"], air["o3"], air["so2"], air["pm2_5"], air["pm10"]
            ]
            writer.writerow(row)

    print(f"✅ Dữ liệu đã được lưu vào {CSV_FILE}")

if __name__ == "__main__":
    crawl_and_save()