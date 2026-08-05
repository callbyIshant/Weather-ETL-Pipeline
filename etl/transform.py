import uuid
from datetime import datetime, timezone
from etl.config import logger

# WMO Weather Code Interpretation Mapping
WMO_WEATHER_CODES = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Depositing rime fog",
    51: "Light drizzle",
    53: "Moderate drizzle",
    55: "Dense drizzle",
    61: "Slight rain",
    63: "Moderate rain",
    65: "Heavy rain",
    71: "Slight snow fall",
    73: "Moderate snow fall",
    75: "Heavy snow fall",
    80: "Slight rain showers",
    81: "Moderate rain showers",
    82: "Violent rain showers",
    95: "Thunderstorm",
    96: "Thunderstorm with slight hail",
    99: "Thunderstorm with heavy hail",
}

def decode_wmo_code(code: int) -> str:
    """Translate WMO numerical code into human-readable condition string."""
    return WMO_WEATHER_CODES.get(code, "Unknown / Other")

def calculate_comfort_index(avg_temp: float, avg_humidity: float) -> str:
    """Calculate a human-friendly comfort category based on temperature and humidity."""
    if avg_temp is None:
        return "Unknown"
    if avg_temp < 5.0:
        return "Very Cold"
    elif avg_temp < 15.0:
        return "Cool"
    elif 15.0 <= avg_temp <= 26.0:
        if avg_humidity and avg_humidity > 75.0:
            return "Humid & Warm"
        return "Pleasant"
    elif 26.0 < avg_temp < 33.0:
        return "Warm / Hot"
    else:
        return "Extreme Heat"

def transform_to_bronze(extracted_data: dict, batch_id: uuid.UUID) -> dict:
    """
    Transform extracted data into Bronze layer schema dictionary.
    """
    return {
        "batch_id": str(batch_id),
        "city": extracted_data["city"],
        "latitude": extracted_data["latitude"],
        "longitude": extracted_data["longitude"],
        "source_url": extracted_data["source_url"],
        "raw_json": extracted_data["raw_payload"],
        "status_code": extracted_data["status_code"],
        "ingested_at": datetime.now(timezone.utc).isoformat()
    }

def transform_to_silver(extracted_data: dict, batch_id: uuid.UUID) -> dict:
    """
    Transform Bronze raw payload into Silver structured time-series datasets.
    Extracts hourly observations and daily aggregations.
    """
    city = extracted_data["city"]
    lat = extracted_data["latitude"]
    lon = extracted_data["longitude"]
    raw = extracted_data["raw_payload"]

    hourly_rows = []
    daily_rows = []

    # 1. Process Hourly Time Series
    if "hourly" in raw:
        h = raw["hourly"]
        times = h.get("time", [])
        for i, ts_str in enumerate(times):
            code = h["weather_code"][i] if "weather_code" in h and i < len(h["weather_code"]) else None
            hourly_rows.append({
                "city": city,
                "latitude": lat,
                "longitude": lon,
                "timestamp": ts_str, # e.g. "2026-08-01T00:00"
                "temperature_2m": h["temperature_2m"][i] if i < len(h["temperature_2m"]) else None,
                "relative_humidity_2m": h["relative_humidity_2m"][i] if i < len(h["relative_humidity_2m"]) else None,
                "dew_point_2m": h["dew_point_2m"][i] if i < len(h["dew_point_2m"]) else None,
                "apparent_temperature": h["apparent_temperature"][i] if i < len(h["apparent_temperature"]) else None,
                "precipitation": h["precipitation"][i] if i < len(h["precipitation"]) else None,
                "rain": h["rain"][i] if i < len(h["rain"]) else None,
                "weather_code": code,
                "weather_condition": decode_wmo_code(code) if code is not None else None,
                "surface_pressure": h["surface_pressure"][i] if i < len(h["surface_pressure"]) else None,
                "wind_speed_10m": h["wind_speed_10m"][i] if i < len(h["wind_speed_10m"]) else None,
                "ingested_batch_id": str(batch_id)
            })

    # 2. Process Daily Time Series
    if "daily" in raw:
        d = raw["daily"]
        dates = d.get("time", [])
        for i, date_str in enumerate(dates):
            code = d["weather_code"][i] if "weather_code" in d and i < len(d["weather_code"]) else None
            daily_rows.append({
                "city": city,
                "date": date_str, # e.g. "2026-08-01"
                "weather_code": code,
                "weather_condition": decode_wmo_code(code) if code is not None else None,
                "temperature_2m_max": d["temperature_2m_max"][i] if i < len(d["temperature_2m_max"]) else None,
                "temperature_2m_min": d["temperature_2m_min"][i] if i < len(d["temperature_2m_min"]) else None,
                "apparent_temperature_max": d["apparent_temperature_max"][i] if i < len(d["apparent_temperature_max"]) else None,
                "apparent_temperature_min": d["apparent_temperature_min"][i] if i < len(d["apparent_temperature_min"]) else None,
                "precipitation_sum": d["precipitation_sum"][i] if i < len(d["precipitation_sum"]) else None,
                "rain_sum": d["rain_sum"][i] if i < len(d["rain_sum"]) else None,
                "wind_speed_10m_max": d["wind_speed_10m_max"][i] if i < len(d["wind_speed_10m_max"]) else None,
                "ingested_batch_id": str(batch_id)
            })

    logger.info(f"Transformed {len(hourly_rows)} hourly rows and {len(daily_rows)} daily rows for Silver layer ({city}).")
    return {
        "hourly": hourly_rows,
        "daily": daily_rows
    }

def transform_silver_to_gold(silver_daily_data: list, silver_hourly_data: list = None) -> dict:
    """
    Transform Silver daily & hourly records into Gold layer business summaries and weather anomalies.
    """
    gold_summaries = []
    gold_anomalies = []

    # Map hourly records by (city, date) to calculate hourly-derived averages (like humidity)
    hourly_by_city_date = {}
    if silver_hourly_data:
        for hr in silver_hourly_data:
            c = hr["city"]
            dt_str = hr["timestamp"].split("T")[0]
            key = (c, dt_str)
            if key not in hourly_by_city_date:
                hourly_by_city_date[key] = []
            hourly_by_city_date[key].append(hr)

    for record in silver_daily_data:
        city = record["city"]
        date_str = record["date"]
        t_max = record["temperature_2m_max"]
        t_min = record["temperature_2m_min"]

        avg_temp = round((t_max + t_min) / 2.0, 2) if (t_max is not None and t_min is not None) else None
        temp_range = round(t_max - t_min, 2) if (t_max is not None and t_min is not None) else None
        precip = record["precipitation_sum"] or 0.0
        wind = record["wind_speed_10m_max"] or 0.0

        # Calculate average humidity from hourly records for that date if available
        hr_records = hourly_by_city_date.get((city, date_str), [])
        humidities = [r["relative_humidity_2m"] for r in hr_records if r.get("relative_humidity_2m") is not None]
        avg_humidity = round(sum(humidities) / len(humidities), 2) if humidities else None

        comfort = calculate_comfort_index(avg_temp, avg_humidity)

        summary_row = {
            "city": city,
            "summary_date": date_str,
            "avg_temp_c": avg_temp,
            "min_temp_c": t_min,
            "max_temp_c": t_max,
            "temp_range_c": temp_range,
            "avg_humidity_pct": avg_humidity,
            "total_precipitation_mm": precip,
            "max_wind_speed_kmh": wind,
            "comfort_index": comfort
        }
        gold_summaries.append(summary_row)

        # Detect Anomalies
        if t_max is not None and t_max >= 35.0:
            gold_anomalies.append({
                "city": city,
                "anomaly_date": date_str,
                "anomaly_type": "HEATWAVE",
                "metric_name": "temperature_2m_max",
                "metric_value": t_max,
                "threshold_value": 35.0,
                "severity": "HIGH" if t_max < 40.0 else "CRITICAL",
                "description": f"Extreme maximum temperature recorded: {t_max}°C (Threshold: >= 35°C)."
            })

        if t_min is not None and t_min <= 0.0:
            gold_anomalies.append({
                "city": city,
                "anomaly_date": date_str,
                "anomaly_type": "FREEZING",
                "metric_name": "temperature_2m_min",
                "metric_value": t_min,
                "threshold_value": 0.0,
                "severity": "MEDIUM" if t_min > -5.0 else "HIGH",
                "description": f"Freezing temperatures recorded: {t_min}°C (Threshold: <= 0°C)."
            })

        if precip >= 20.0:
            gold_anomalies.append({
                "city": city,
                "anomaly_date": date_str,
                "anomaly_type": "HEAVY_RAIN",
                "metric_name": "precipitation_sum",
                "metric_value": precip,
                "threshold_value": 20.0,
                "severity": "HIGH" if precip < 50.0 else "CRITICAL",
                "description": f"Heavy rainfall accumulation: {precip}mm (Threshold: >= 20mm)."
            })

        if wind >= 45.0:
            gold_anomalies.append({
                "city": city,
                "anomaly_date": date_str,
                "anomaly_type": "HIGH_WIND",
                "metric_name": "wind_speed_10m_max",
                "metric_value": wind,
                "threshold_value": 45.0,
                "severity": "HIGH",
                "description": f"High wind speeds detected: {wind} km/h (Threshold: >= 45 km/h)."
            })

    logger.info(f"Transformed {len(gold_summaries)} daily summary records and detected {len(gold_anomalies)} anomalies for Gold layer.")
    return {
        "summaries": gold_summaries,
        "anomalies": gold_anomalies
    }
