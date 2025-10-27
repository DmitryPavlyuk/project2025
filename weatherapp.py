
import streamlit as st
import requests
from datetime import datetime
import pytz
import json
import pandas as pd

st.title("🌦 Weather App with Google APIs")

# Link to Streamlit - https://weatherapp-yg3fjryl2kym6cewxqv6ca.streamlit.app 
# Replace with your actual API keys or use st.secrets
# api_key = st.secrets["GOOGLE_API_KEY"]
# weather_api = st.secrets["G_WEATHER_API"]

api_key = "AIzaSyAX3v9OSj4Fg3Ad649BIRR13B09CidYNqc"
weather_api = "AIzaSyBF5T-bQzl9-NFJ_aHMzTgTntqC-TjMIw4"

# Helper functions
def safe_get(d, *keys, default=None):
    """Safely traverse nested dicts."""
    for k in keys:
        if not isinstance(d, dict) or k not in d:
            return default
        d = d[k]
    return d

def format_time(iso_ts: str, tz_id: str | None = None):
    """Format ISO timestamp (assumes Z or +00:00)."""
    if not iso_ts:
        return None
    try:
        dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return iso_ts

def display_weather(data: dict, city_name: str = ""):
    """Parse and display weather data in a clean layout."""
    # top-level fields
    current_time = safe_get(data, "currentTime")
    timezone_id = safe_get(data, "timeZone", "id")
    is_day = safe_get(data, "isDaytime")

    cond_text = safe_get(data, "weatherCondition", "description", "text", default="N/A")
    icon_uri = safe_get(data, "weatherCondition", "iconBaseUri")
    condition_type = safe_get(data, "weatherCondition", "type")

    temp = safe_get(data, "temperature", "degrees", default="N/A")
    temp_unit = safe_get(data, "temperature", "unit", default="CELSIUS")

    feels_like = safe_get(data, "feelsLikeTemperature", "degrees", default="N/A")
    humidity = safe_get(data, "relativeHumidity", default="N/A")
    dew_point = safe_get(data, "dewPoint", "degrees", default="N/A")
    wind_speed = safe_get(data, "wind", "speed", "value", default="N/A")
    wind_unit = safe_get(data, "wind", "speed", "unit", default="")
    wind_dir = safe_get(data, "wind", "direction", "cardinal", default="N/A")
    wind_gust = safe_get(data, "wind", "gust", "value")
    visibility = safe_get(data, "visibility", "distance", default="N/A")
    visibility_unit = safe_get(data, "visibility", "unit", default="")
    pressure = safe_get(data, "airPressure", "meanSeaLevelMillibars", default="N/A")
    uv = safe_get(data, "uvIndex", default="N/A")
    precip_prob = safe_get(data, "precipitation", "probability", "percent", default="N/A")
    precip_qpf = safe_get(data, "precipitation", "qpf", "quantity", default="N/A")
    precip_unit = safe_get(data, "precipitation", "qpf", "unit", default="")
    cloud_cover = safe_get(data, "cloudCover", default="N/A")

    temp_min = safe_get(data, "currentConditionsHistory", "minTemperature", "degrees")
    temp_max = safe_get(data, "currentConditionsHistory", "maxTemperature", "degrees")
    qpf_history = safe_get(data, "currentConditionsHistory", "qpf", "quantity")


    # Header
    st.subheader(f"🌍 Current Weather{(' — ' + city_name) if city_name else ''}")
    formatted_time = format_time(current_time, timezone_id)
    if formatted_time:
        st.caption(f"Updated: {formatted_time}")

    # Main row: icon + main metrics
    icon_col, main_col = st.columns([1, 4])
    with main_col:
        st.markdown(f"### {temp}° {temp_unit.capitalize()} — **{cond_text}**")
        st.write(f"Feels like: **{feels_like}° {temp_unit.capitalize()}**")
        if condition_type:
            st.write(f"*Condition type:* `{condition_type}`")


    st.write("---")

    # Key metrics
    c1, c2, c3 = st.columns(3)
    c1.metric("💧 Humidity", f"{humidity}%", delta=None)
    c2.metric("🌬 Wind", f"{wind_speed} {wind_unit}", delta=wind_dir)
    c3.metric("🔆 UV Index", f"{uv}", delta=None)

    # Secondary info
    s1, s2, s3 = st.columns(3)
    s1.write(f"**Dew point:** {dew_point}° {temp_unit.capitalize()}")
    s2.write(f"**Pressure (MSL):** {pressure} mb")
    s3.write(f"**Visibility:** {visibility} {visibility_unit}")

    st.write(f"**Precipitation probability:** {precip_prob}%")
    st.write(f"**Precipitation (QPF):** {precip_qpf} {precip_unit}")
    st.write(f"**Cloud cover:** {cloud_cover}%")

    if wind_gust is not None:
        st.write(f"**Wind gust:** {wind_gust} {wind_unit}")

    if any(v is not None for v in (temp_min, temp_max, qpf_history)):
        st.write("---")
        st.write("**Summary (history)**")
        if temp_max is not None:
            st.write(f"- Max temp: {temp_max}° {temp_unit}")
        if temp_min is not None:
            st.write(f"- Min temp: {temp_min}° {temp_unit}")
        if qpf_history is not None:
            st.write(f"- Total QPF (history): {qpf_history} {precip_unit or 'mm'}")


    st.caption("Data source: Google Weather API")

def display_forecast(forecast: dict, city_name: str = ""):
    """Parse and display weather data in a clean layout."""
    # top-level fields
    if "forecastDays" in forecast:
            forecast_list = []
            for day in forecast["forecastDays"]:
                date_str = f"{day['displayDate']['year']}-{day['displayDate']['month']:02d}-{day['displayDate']['day']:02d}"
                date = datetime.strptime(date_str, "%Y-%m-%d").date()

                # Extract key fields
                daytime = day["daytimeForecast"]
                nighttime = day["nighttimeForecast"]

                forecast_list.append({
                    "Date": date,
                    "Condition (Day)": daytime["weatherCondition"]["description"]["text"],
                    "Condition (Night)": nighttime["weatherCondition"]["description"]["text"],
                    "Max Temp (°C)": day["maxTemperature"]["degrees"],
                    "Min Temp (°C)": day["minTemperature"]["degrees"],
                    "Feels Like Max (°C)": day["feelsLikeMaxTemperature"]["degrees"],
                    "Feels Like Min (°C)": day["feelsLikeMinTemperature"]["degrees"],
                    "Humidity (Day)": daytime.get("relativeHumidity", None),
                    "Humidity (Night)": nighttime.get("relativeHumidity", None),
                    "Precip Chance (Day)": daytime["precipitation"]["probability"]["percent"],
                    "Precip Chance (Night)": nighttime["precipitation"]["probability"]["percent"],
                    "Wind (Day)": f"{daytime['wind']['speed']['value']} {daytime['wind']['speed']['unit']}",
                    "Wind (Night)": f"{nighttime['wind']['speed']['value']} {nighttime['wind']['speed']['unit']}",
                })

            # --- Convert to DataFrame ---
            df = pd.DataFrame(forecast_list)

            # --- Display nicely ---
            st.subheader("🌤 5-Day Weather Forecast")
            st.dataframe(df, use_container_width=True)

    else:
            st.warning("No forecast data available in the response.")

# --- UI Inputs ---
city_name = st.text_input("Enter your city:", "Riga")

if st.button("Get Weather"):
    try:
        # 1) Geocode: city -> lat/lon
        geo_url = "https://maps.googleapis.com/maps/api/geocode/json"
        geo_params = {"address": city_name, "key": api_key}
        geo_response = requests.get(geo_url, params=geo_params, timeout=10)
        geo_response.raise_for_status()
        geo_data = geo_response.json()

        results = geo_data.get("results")
        if not results:
            st.warning(f"No location found for '{city_name}'. Please try another name.")
            st.stop()

        loc = results[0]["geometry"]["location"]
        lat, lon = loc["lat"], loc["lng"]
        st.write(f"📍 Latitude: {lat}, Longitude: {lon}")

        # 2) Weather: lat/lon -> current conditions
        weather_url = "https://weather.googleapis.com/v1/currentConditions:lookup"
        weather_params = {
            "key": weather_api,
            "location.latitude": lat,
            "location.longitude": lon
        }
        weather_response = requests.get(weather_url, params=weather_params, timeout=10)
        weather_response.raise_for_status()
        data = weather_response.json()
        
        # 3) Forcast weather for 5 days
        forecast_url = "https://weather.googleapis.com/v1/forecast/days:lookup"  # Example forecast endpoint
        forecast_params = {
            "key": weather_api,
            "location.latitude": lat,
            "location.longitude": lon,
            "days": 5 # Assuming a parameter for number of days
        }
        forecast_response = requests.get(forecast_url, params=forecast_params, timeout=10)
        forecast_response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        forecast = forecast_response.json()
        # Display parsed data
        display_weather(data, city_name=city_name)
        display_forecast(forecast, city_name=city_name)
        #forecast = get_forecast(lat, lon, weather_api)
        #st.write(forecast) -- for debugging
        

    except requests.HTTPError as he:
        st.error(f"HTTP error: {he}")
    except requests.RequestException as re:
        st.error(f"Network error: {re}")
    except Exception as e:
        st.error(f"Unexpected error: {e}")
