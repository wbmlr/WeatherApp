import requests
import datetime
from datetime import timezone
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from collections import deque

# Load environment variables
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
                # Calculate time to wait until the oldest call falls out of the window
                time_to_wait = self.calls_timestamps[0] - (now - self.interval) + 0.01 # Add a small buffer
                time.sleep(time_to_wait)

def get_historical_weather_for_day(lat, lon, dt_unix_timestamp, api_key, rate_limiter):
    """Fetches historical weather data for a single day with rate limiting."""
    rate_limiter.wait_for_slot() # Wait before making the call
    url = f"https://api.openweathermap.org/data/3.0/onecall/timemachine?lat={lat}&lon={lon}&dt={dt_unix_timestamp}&appid={api_key}&units=metric"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return dt_unix_timestamp, response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for timestamp {dt_unix_timestamp}: {e}")
        return dt_unix_timestamp, None

def get_historical_weather_in_range_concurrently(lat, lon, start_date_str, end_date_str, api_key):
    """
    Fetches historical weather data for a date range concurrently using ThreadPoolExecutor.
    Enforces a rate limit of 60 calls per minute and a total limit of 240 calls.

    Args:
        lat (float): Latitude.
        lon (float): Longitude.
        start_date_str (str): Start date in 'YYYY-MM-DD' format.
        end_date_str (str): End date in 'YYYY-MM-DD' format.
        api_key (str): Your OpenWeatherMap API key.

    Returns:
        dict: A dictionary where keys are Unix timestamps and values are the API responses.
    """
    start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').date()
    end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').date()

    if not API_KEY:
        print("Error: OpenWeatherMap API key not provided.")
        return {}

    all_timestamps = []
    current_day = start_date
    while current_day <= end_date:
        daily_timestamp = int(datetime.datetime(current_day.year, current_day.month, current_day.day, 12, 0, 0, tzinfo=timezone.utc).timestamp()) # Use the imported timezone directly
        print('timezone.utc: ', timezone.utc, 'daily_timestamp: ', daily_timestamp)
        all_timestamps.append(daily_timestamp)
        current_day += datetime.timedelta(days=1)

    results = {}
    calls_made = 0
    max_total_calls = 240
    rate_limiter = RateLimiter(calls_per_minute=60)

    # Use ThreadPoolExecutor for concurrent API calls
    # Max workers should be less than or equal to calls_per_minute to avoid overwhelming the rate limiter
    with ThreadPoolExecutor(max_workers=10) as executor: # Adjusted max_workers
        futures = []
        for ts in all_timestamps:
            if calls_made >= max_total_calls:
                break
            futures.append(executor.submit(get_historical_weather_for_day, lat, lon, ts, api_key, rate_limiter))
            calls_made += 1 # Increment immediately when submitted

        for future in as_completed(futures):
            try:
                dt_ts, data = future.result()
                if data:
                    results[dt_ts] = data
                else:
                    print(f"Skipping data for {datetime.datetime.fromtimestamp(dt_ts).date()} due to error.")
            except TimeoutError:
                print("A future timed out, likely due to rate limiting or network issues.")
            except Exception as exc:
                print(f"Future generated an exception: {exc}")
    return results

if __name__ == "__main__":
    example_lat = 51.5074
    example_lon = -0.1278
    # Test with a range that would exceed 240 days to see the limit
    start_date = "2025-05-20" 
    end_date = "2025-05-30" 

    print(f"Fetching historical weather for {start_date} to {end_date} concurrently with rate limits...")
    historical_data_collection = get_historical_weather_in_range_concurrently(
        example_lat, example_lon, start_date, end_date, API_KEY
    )

    if historical_data_collection:
        print(f"\n--- Summary of fetched data ({len(historical_data_collection)} entries) ---")
        for timestamp, data in sorted(historical_data_collection.items()):
            date_str = datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc).strftime('%Y-%m-%d')
            temp = data['data'][0]['temp'] if data and 'data' in data and data['data'] else 'N/A'
            print(f"Date: {date_str}, First hour temp: {temp}°C")
    else:
        print("No historical data retrieved.")
