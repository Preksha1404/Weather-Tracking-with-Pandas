from dotenv import load_dotenv
import os
import requests
import pandas as pd
import schedule
import time

load_dotenv()

API_KEY=os.getenv("API_KEY")

# Function to get weather data and save to CSV
def get_weather_data(city_name,state_code,country_code,limit):
    url = f"http://api.openweathermap.org/geo/1.0/direct?q={city_name},{state_code},{country_code}&limit={limit}&appid={API_KEY}"
    response = requests.get(url)
    city_data=response.json()

    city = city_data[0]
    lat = city["lat"]
    lon = city["lon"]

    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}"
    response = requests.get(url)

    weather_data=response.json()

    weather = {
        "city": weather_data.get("name"),
        "country": weather_data.get("sys", {}).get("country"),
        "timezone": weather_data.get("timezone"),
        "weather_main": weather_data.get("weather", [{}])[0].get("main"),
        "weather_description": weather_data.get("weather", [{}])[0].get("description"),
        "temp": weather_data.get("main", {}).get("temp"),
        "feels_like": weather_data.get("main", {}).get("feels_like"),
        "temp_min": weather_data.get("main", {}).get("temp_min"),
        "temp_max": weather_data.get("main", {}).get("temp_max"),
        "pressure": weather_data.get("main", {}).get("pressure"),
        "humidity": weather_data.get("main", {}).get("humidity"),
        "visibility": weather_data.get("visibility"),
        "wind_speed": weather_data.get("wind", {}).get("speed"),
        "sunrise": weather_data.get("sys", {}).get("sunrise"),
        "sunset": weather_data.get("sys", {}).get("sunset"),
        "timestamp": weather_data.get("dt"),
    }

    # print(type(weather))
    # print([weather])

    df=pd.DataFrame(weather,index=[0])
    print(df)

    file_path = "weather.csv"
    if os.path.exists(file_path):
        df.to_csv(file_path, mode="a", header=False, index=False)
    else:
        df.to_csv(file_path, mode="w", header=True, index=False)

    print("Weather data added!")

# Schedule the task to run daily at 8 AM
def main():
    get_weather_data(city_name="Ahmedabad",state_code="GJ",country_code="IN",limit=5)

# schedule.every().second.do(main)
# schedule.every().minute.do(main)
schedule.every().day.at("08:00").do(main)

# Keep the script running
while True:
    schedule.run_pending()
    time.sleep(1)