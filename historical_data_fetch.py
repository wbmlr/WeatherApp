# historical_data_fetch.py
import requests
import datetime
from datetime import timezone
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from collections import deque

from dotenv import load_dotenv
load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")

class RateLimiter:
    def __init__(self, calls_per_minute):
        self.calls_per_minute = calls_per_minute
        self.calls_timestamps = deque()
        self.interval = 60 # seconds

    def wait_for_slot(self):
        while True:
            now = time.time()
            # Remove timestamps older than the interval
            while self.calls_timestamps and self.calls_timestamps[0] <= now - self.interval:
                self.calls_timestamps.popleft()

            if len(self.calls_timestamps) < self.calls_per_minute:
                self.calls_timestamps.append(now)
                break
            else:
                time_to_wait = self.calls_timestamps[0] - (now - self.interval) + 0.01 # Add a small buffer
                time.sleep(time_to_wait)

def get_historical_weather_for_day(lat, lon, dt_unix_timestamp, api_key, rate_limiter):
    """Fetches historical weather data for a single day with rate limiting."""
    rate_limiter.wait_for_slot()
    url = f"https://api.openweathermap.org/data/3.0/onecall/timemachine?lat={lat}&lon={lon}&dt={dt_unix_timestamp}&appid={api_key}&units=metric"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return dt_unix_timestamp, response.json()
    except requests.exceptions.RequestException as e:
        # print(f"Error fetching data for timestamp {dt_unix_timestamp}: {e}") # Keep printing for debugging in console
        return dt_unix_timestamp, None

# MODIFIED: Accepts a list of specific timestamps
def get_historical_weather_for_specific_timestamps_concurrently(lat, lon, timestamps_to_fetch, api_key):
    """
    Fetches historical weather data for specific Unix timestamps concurrently.
    Enforces a rate limit of 60 calls per minute and a total limit of 240 calls.

    Args:
        lat (float): Latitude.
        lon (float): Longitude.
        timestamps_to_fetch (list): List of Unix timestamps (integer) for the days to fetch.
        api_key (str): Your OpenWeatherMap API key.

    Returns:
        dict: A dictionary where keys are Unix timestamps and values are the API responses.
    """
    if not api_key: # Use passed api_key, not global API_KEY here
        print("Error: OpenWeatherMap API key not provided to historical_data_fetch.")
        return {}

    results = {}
    max_total_calls = 240
    rate_limiter = RateLimiter(calls_per_minute=60)

    # Limit the number of timestamps to fetch based on max_total_calls
    timestamps_to_process = timestamps_to_fetch[:max_total_calls]

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for ts in timestamps_to_process:
            futures.append(executor.submit(get_historical_weather_for_day, lat, lon, ts, api_key, rate_limiter))

        for future in as_completed(futures):
            try:
                dt_ts, data = future.result()
                if data:
                    results[dt_ts] = data
                else:
                    # print(f"Skipping data for {datetime.datetime.fromtimestamp(dt_ts).date()} due to error.") # Keep printing for debugging in console
                    pass
            except TimeoutError:
                # print("A future timed out, likely due to rate limiting or network issues.") # Keep printing for debugging in console
                pass
            except Exception as exc:
                # print(f"Future generated an exception: {exc}") # Keep printing for debugging in console
                pass
    return results

if __name__ == "__main__":
    example_lat = 51.5074
    example_lon = -0.1278
    # Test with a range
    start_date_test = datetime.date(2025, 5, 20)
    end_date_test = datetime.date(2025, 5, 30)

    timestamps_for_test = []
    current_day_test = start_date_test
    while current_day_test <= end_date_test:
        timestamps_for_test.append(int(datetime.datetime(current_day_test.year, current_day_test.month, current_day_test.day, 12, 0, 0, tzinfo=timezone.utc).timestamp()))
        current_day_test += datetime.timedelta(days=1)

    print(f"Fetching historical weather for {len(timestamps_for_test)} days concurrently with rate limits...")
    historical_data_collection = get_historical_weather_for_specific_timestamps_concurrently(
        example_lat, example_lon, timestamps_for_test, API_KEY
    )

    if historical_data_collection:
        print(f"\n--- Summary of fetched data ({len(historical_data_collection)} entries) ---")
        for timestamp, data in sorted(historical_data_collection.items()):
            date_str = datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc).strftime('%Y-%m-%d')
            temp = data['data'][0]['temp'] if data and 'data' in data and data['data'] else 'N/A'
            print(f"Date: {date_str}, First hour temp: {temp}°C")
    else:
        print("No historical data retrieved.")