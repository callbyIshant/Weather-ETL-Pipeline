import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from etl.config import logger, CITY_CATALOG, FETCH_DAYS

OPEN_METEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"

def get_http_session(retries=5, backoff_factor=1.5):
    """
    Create a requests Session with HTTP retry logic and exponential backoff.
    """
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def fetch_weather_for_city(city_name: str, session: requests.Session = None) -> dict:
    """
    Fetch weather metrics (hourly & daily) for a specified city from Open-Meteo REST API.
    """
    if city_name not in CITY_CATALOG:
        raise ValueError(f"City '{city_name}' not found in CITY_CATALOG.")

    coords = CITY_CATALOG[city_name]
    params = {
        "latitude": coords["lat"],
        "longitude": coords["lon"],
        "hourly": ",".join([
            "temperature_2m",
            "relative_humidity_2m",
            "dew_point_2m",
            "apparent_temperature",
            "precipitation",
            "rain",
            "weather_code",
            "surface_pressure",
            "wind_speed_10m"
        ]),
        "daily": ",".join([
            "weather_code",
            "temperature_2m_max",
            "temperature_2m_min",
            "apparent_temperature_max",
            "apparent_temperature_min",
            "precipitation_sum",
            "rain_sum",
            "wind_speed_10m_max"
        ]),
        "past_days": FETCH_DAYS,
        "forecast_days": 1,
        "timezone": "UTC"
    }

    if session is None:
        session = get_http_session()

    logger.info(f"Extracting weather data from Open-Meteo for {city_name} (Lat: {coords['lat']}, Lon: {coords['lon']})...")
    try:
        response = session.get(OPEN_METEO_BASE_URL, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Successfully extracted weather data for {city_name}.")
        return {
            "city": city_name,
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "source_url": response.url,
            "status_code": response.status_code,
            "raw_payload": data
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch weather data for {city_name} from Open-Meteo API: {e}")
        raise
