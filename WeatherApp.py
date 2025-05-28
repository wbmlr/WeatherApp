# WeatherApp.py
import streamlit as st
import requests
from datetime import datetime, timedelta, date, timezone
import json
import db_cache
import os
from COUNTRIES import COUNTRIES
from streamlit_geolocation import streamlit_geolocation
import folium
from streamlit_folium import folium_static
import pandas as pd
import altair as alt
import uuid
import historical_data_fetch
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")
GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY") # Ensure this is used if needed elsewhere

st.set_page_config(layout="centered", page_title="Weather App")
st.title("Current Weather & Historical Forecast")

db_cache.init_db()

# Initialize session state
if 'weather_data' not in st.session_state: st.session_state.weather_data = None
if 'location_display' not in st.session_state: st.session_state.location_display = None
if 'current_lat' not in st.session_state: st.session_state.current_lat = None
if 'current_lon' not in st.session_state: st.session_state.current_lon = None
if 'session_id' not in st.session_state: st.session_state.session_id = str(uuid.uuid4())
if 'historical_temps' not in st.session_state: st.session_state.historical_temps = None
if 'selected_db_table_view' not in st.session_state: st.session_state.selected_db_table_view = None
if 'city_search_term' not in st.session_state: st.session_state.city_search_term = ""
if 'weather_type_session' not in st.session_state: st.session_state.weather_type_session = "Current Weather"


# NEW: Cached function to fetch and process all weather data
@st.cache_data(ttl=3600) # Cache for 1 hour
def _get_weather_data_cached(location_type, location_input_value, manual_lat, manual_lon,
                             weather_type_selected, start_date_selected, end_date_selected, api_key_val):
    
    _weather_data = None
    _historical_temps = None
    _location_display = None
    _error_message = None

    _lat, _lon = manual_lat, manual_lon # Start with potentially pre-filled GPS coords

    try:
        # --- Step 1: Get Lat/Lon for the location_input_value ---
        if location_type == "City Name":
            geo_url = f"http://api.openweathermap.org/data/2.5/weather?q={location_input_value}&appid={api_key_val}"
            geo_response = requests.get(geo_url).json()
            if geo_response.get("cod") == 200 and 'coord' in geo_response:
                _lat = geo_response['coord']['lat']
                _lon = geo_response['coord']['lon']
                _location_display = f"Coordinates for '{location_input_value}': Lat={_lat}, Lon={_lon}"
            else:
                _error_message = f"Location not found or API error: {geo_response.get('message', 'Unknown error')}"
                return None, None, None, _error_message
        elif location_type == "Zip Code":
            geo_url = f"http://api.openweathermap.org/data/2.5/weather?zip={location_input_value}&appid={api_key_val}"
            geo_response = requests.get(geo_url).json()
            if geo_response.get("cod") == 200 and 'coord' in geo_response:
                _lat = geo_response['coord']['lat']
                _lon = geo_response['coord']['lon']
                _location_display = f"Coordinates for '{location_input_value}': Lat={_lat}, Lon={_lon}"
            else:
                _error_message = f"Location not found or API error: {geo_response.get('message', 'Unknown error')}"
                return None, None, None, _error_message
        elif location_type == "GPS Coordinates":
            if _lat is None or _lon is None:
                _error_message = "GPS Coordinates not provided or invalid."
                return None, None, None, _error_message
            _location_display = f"Coordinates: Lat={_lat}, Lon={_lon}"

        if _lat is None or _lon is None:
            _error_message = "Could not determine location coordinates."
            return None, None, None, _error_message

        # --- Step 2: Log User Query (Called here, so it's logged when data is *initially* fetched/cached)
        # Note: If the result is served from st.cache_data, this log won't run again for the same query.
        start_ts_log = int(datetime.combine(start_date_selected, datetime.min.time()).timestamp()) if start_date_selected and weather_type_selected == "Historical Weather" else None
        end_ts_log = int(datetime.combine(end_date_selected, datetime.min.time()).timestamp()) if end_date_selected and weather_type_selected == "Historical Weather" else None
        db_cache.log_user_query(st.session_state.session_id, location_input_value, start_ts_log, end_ts_log)

        # --- Step 3: Fetch Weather Data (Current or Historical) ---
        if weather_type_selected == "Current Weather":
            cached_data = db_cache.get_cache(_lat, _lon, None, location_input_value)
            if cached_data:
                _weather_data = cached_data
            else:
                onecall_url = f"https://api.openweathermap.org/data/3.0/onecall?lat={_lat}&lon={_lon}&exclude=minutely,hourly,alerts&appid={api_key_val}&units=metric"
                fetched_data = requests.get(onecall_url).json()
                if fetched_data.get("cod") == 200 or "current" in fetched_data:
                    _weather_data = fetched_data
                    data_ts = fetched_data['current']['dt'] if 'current' in fetched_data and 'dt' in fetched_data['current'] else int(datetime.now().timestamp())
                    st.info(f"Data for: {datetime.fromtimestamp(data_ts).strftime('%Y-%m-%d %H:%M:%S')} fetched from API.")
                    db_cache.set_cache(_lat, _lon, location_input_value, fetched_data, data_ts)
                else:
                    _error_message = f"Failed to fetch current weather: {fetched_data.get('message', 'Unknown error')}"
                    return None, None, None, _error_message

        elif weather_type_selected == "Historical Weather":
            if not start_date_selected or not end_date_selected:
                _error_message = "Please select both start and end dates for historical weather."
                return None, None, None, _error_message

            historical_temps_raw_data = {}
            
            start_ts_at_midnight = int(datetime(start_date_selected.year, start_date_selected.month, start_date_selected.day, 0, 0, 0).timestamp())
            end_ts_at_midnight = int(datetime(end_date_selected.year, end_date_selected.month, end_date_selected.day, 0, 0, 0).timestamp())

            cached_data_for_range = db_cache.get_cache_for_range(_lat, _lon, start_ts_at_midnight, end_ts_at_midnight)
            for ts, data in cached_data_for_range.items():
                historical_temps_raw_data[ts] = data

            missing_api_timestamps = []
            current_day_iter = start_date_selected
            while current_day_iter <= end_date_selected:
                daily_timestamp_for_db_key = int(datetime(current_day_iter.year, current_day_iter.month, current_day_iter.day, 0, 0, 0).timestamp())
                
                if daily_timestamp_for_db_key not in historical_temps_raw_data:
                    # Append noon UTC timestamp for API call
                    missing_api_timestamps.append(int(datetime(current_day_iter.year, current_day_iter.month, current_day_iter.day, 12, 0, 0, tzinfo=timezone.utc).timestamp()))
                current_day_iter += timedelta(days=1)
            
            if missing_api_timestamps:
                api_fetched_data = historical_data_fetch.get_historical_weather_for_specific_timestamps_concurrently(
                    _lat, _lon, missing_api_timestamps, api_key_val
                )
                
                for timestamp_noon_utc, data in api_fetched_data.items():
                    api_date = datetime.fromtimestamp(timestamp_noon_utc, tz=timezone.utc).date()
                    db_key_timestamp = int(datetime(api_date.year, api_date.month, api_date.day, 0, 0, 0).timestamp())
                    
                    if db_key_timestamp not in historical_temps_raw_data: # Ensure we don't overwrite existing cache
                        historical_temps_raw_data[db_key_timestamp] = data
                        db_cache.set_cache(_lat, _lon, location_input_value, data, db_key_timestamp)

                fetched_timestamps_str = ", ".join([datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S') for ts in api_fetched_data.keys()])
                if fetched_timestamps_str:
                    st.info(f"Data for: {fetched_timestamps_str} fetched from API.")
                else:
                    st.info('All Data from DataBase.')

            historical_temps_list = []
            current_day_iter_final = start_date_selected
            while current_day_iter_final <= end_date_selected:
                daily_timestamp_for_db_key = int(datetime(current_day_iter_final.year, current_day_iter_final.month, current_day_iter_final.day, 0, 0, 0).timestamp())
                
                day_data = historical_temps_raw_data.get(daily_timestamp_for_db_key)

                if day_data and 'data' in day_data and len(day_data['data']) > 0:
                    temps_for_day = [hour['temp'] for hour in day_data['data'] if 'temp' in hour]
                    avg_temp = sum(temps_for_day) / len(temps_for_day) if temps_for_day else None
                    
                    if avg_temp is not None:
                        historical_temps_list.append({
                            "Date": current_day_iter_final,
                            "Average Temperature (°C)": round(avg_temp, 1)
                        })
                current_day_iter_final += timedelta(days=1)
            
            if historical_temps_list:
                _historical_temps = pd.DataFrame(sorted(historical_temps_list, key=lambda x: x['Date']))
            else:
                _error_message = "No historical temperature data could be retrieved for the selected range."

    except requests.exceptions.RequestException as e:
        _error_message = f"Network error during weather fetch: {e}"
    except Exception as e:
        _error_message = f"An unexpected error occurred during weather fetch: {e}"

    return _weather_data, _historical_temps, _location_display, _error_message


location_type = st.radio("Select input type:", ("City Name", "Zip Code", "GPS Coordinates"))
st.session_state.weather_type_session = st.radio("Select weather type:", ("Current Weather", "Historical Weather")) # Use session state for weather_type

location_input_value = ""
lat, lon = None, None

# --- Location Input Logic ---
if location_type == "GPS Coordinates":
    geolocation_data = streamlit_geolocation()
    if geolocation_data and geolocation_data.get('latitude') and geolocation_data.get('longitude'):
        lat = st.session_state.current_lat = geolocation_data['latitude']
        lon = st.session_state.current_lon = geolocation_data['longitude']
        location_input_value = f"{st.session_state.current_lat},{st.session_state.current_lon}"
        st.info(f"Using coordinates from GPS: Lat={st.session_state.current_lat}, Lon={st.session_state.current_lon}")

        if 'show_map' not in st.session_state:
            st.session_state.show_map = False

        if st.button('Toggle Map View'):
            st.session_state.show_map = not st.session_state.show_map

        if st.session_state.show_map:
            st.subheader("Location Map")
            m = folium.Map(location=[lat, lon], zoom_start=18)
            folium.Marker([lat, lon]).add_to(m)
            folium_static(m)
    else:
        # Use session state for manual GPS input to persist across reruns
        manual_gps_input_key = "manual_gps_input"
        if 'manual_gps_val' not in st.session_state:
            st.session_state.manual_gps_val = ""
        
        st.session_state.manual_gps_val = st.text_input("Enter coordinates (Lat, Lon)", value=st.session_state.manual_gps_val, key=manual_gps_input_key)
        
        if st.session_state.manual_gps_val:
            try:
                lat_str, lon_str = map(str.strip, st.session_state.manual_gps_val.split(','))
                lat = st.session_state.current_lat = float(lat_str)
                lon = st.session_state.current_lon = float(lon_str)
                location_input_value = st.session_state.manual_gps_val
            except ValueError:
                st.error("Invalid coordinate format. Use 'Lat,Lon'.")
                # Do not st.stop() here, allow the rest of the app to render.
        else:
            st.session_state.current_lat = None
            st.session_state.current_lon = None
else:
    filtered_country_names = [name for name in COUNTRIES.keys()]
    selected_country_name = st.selectbox("Select Country", filtered_country_names, key="country_select")
    selected_country_code = COUNTRIES.get(selected_country_name)
    
    if selected_country_code:
        if location_type == "Zip Code":
            zip_code = st.text_input(f"Enter Zip Code for {selected_country_name}", key="zip_input")
            if zip_code: location_input_value = f"{zip_code},{selected_country_code}"
        else: # City Name
            city_name = st.text_input(f"Enter City Name in {selected_country_name}", "Hyderabad", key="city_input")
            if city_name: location_input_value = f"{city_name},{selected_country_code}"
    else: st.info("Please select a country.")

# --- Date Range Input for Historical Weather ---
start_date, end_date = None, None
if st.session_state.weather_type_session == "Historical Weather":
    st.subheader("Select Date Range")
    today = datetime.now().date()
    max_future_date = today + timedelta(days=500)
    default_future_date = today + timedelta(days=10)

    max_past_date = today - timedelta(days=5000)
    default_past_date = today - timedelta(days=10)
    
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", max_value=today, min_value=max_past_date, value=default_past_date)
    with col2:
        end_date = st.date_input("End Date", max_value=max_future_date, min_value=start_date, value=default_future_date)


# Use a unique key for the button to prevent issues with re-runs if other inputs change
if st.button("Get Weather", key="get_weather_main_button"):
    st.session_state.weather_data = None
    st.session_state.location_display = None
    st.session_state.historical_temps = None
    st.session_state.selected_db_table_view = None # Clear this specific view

    if not location_input_value and location_type != "GPS Coordinates": # GPS coordinates can be derived from lat/lon directly
        st.warning("Please enter a valid location or select required options.")
        st.stop()
    if location_type == "GPS Coordinates" and (lat is None or lon is None):
        st.warning("Please provide valid GPS coordinates.")
        st.stop()
    if not API_KEY or API_KEY == "YOUR_DEFAULT_API_KEY_HERE_OR_READ_FROM_ENV":
        st.error("Please provide a valid OpenWeatherMap API key.")
        st.stop()
    if st.session_state.weather_type_session == "Historical Weather" and (start_date is None or end_date is None):
        st.warning("Please select both start and end dates for historical weather.")
        st.stop()

    with st.spinner("Fetching location and weather data..."):
        # Call the cached function
        fetched_weather_data, fetched_historical_temps, fetched_location_display, error_msg = \
            _get_weather_data_cached(location_type, location_input_value, lat, lon,
                                     st.session_state.weather_type_session, start_date, end_date, API_KEY)
        
        if error_msg:
            st.error(error_msg)
            # Clear session state if there was an error to prevent displaying stale data
            st.session_state.weather_data = None
            st.session_state.historical_temps = None
            st.session_state.location_display = None
        else:
            st.session_state.weather_data = fetched_weather_data
            st.session_state.historical_temps = fetched_historical_temps
            st.session_state.location_display = fetched_location_display

# --- Display Fetched Data (unchanged) ---
if st.session_state.location_display:
    st.success(st.session_state.location_display)

if st.session_state.weather_type_session == "Current Weather" and st.session_state.weather_data:
    st.header("Current Weather")
    if 'current' in st.session_state.weather_data:
        current_data = st.session_state.weather_data['current']
        current_desc = current_data['weather'][0]['description']
        st.info(f"Condition: {current_desc.capitalize()}")

        ALL_CURRENT_METRICS = {
            "temp": {"label": "Temperature", "unit": "°C"}, "feels_like": {"label": "Feels Like", "unit": "°C"},
            "pressure": {"label": "Pressure", "unit": " hPa"}, "humidity": {"label": "Humidity", "unit": "%"},
            "dew_point": {"label": "Dew Point", "unit": "°C"}, "uvi": {"label": "UV Index", "unit": ""},
            "clouds": {"label": "Clouds", "unit": "%"}, "visibility": {"label": "Visibility", "unit": " meters"},
            "wind_speed": {"label": "Wind Speed", "unit": " m/s"}, "wind_deg": {"label": "Wind Direction", "unit": "°"},
            "sunrise": {"label": "Sunrise", "unit": "", "format_func": lambda ts: datetime.fromtimestamp(ts).strftime('%H:%M')},
            "sunset": {"label": "Sunset", "unit": "", "format_func": lambda ts: datetime.fromtimestamp(ts).strftime('%H:%M')}
        }
        default_selected_metrics = ["temp", "feels_like", "pressure", "humidity", "visibility"]
        selected_metrics_keys = st.multiselect(
            "Select current weather details to show:", options=list(ALL_CURRENT_METRICS.keys()),
            default=default_selected_metrics,
            format_func=lambda x: ALL_CURRENT_METRICS[x]["label"]
        )
        num_columns = 5
        cols = st.columns(num_columns)
        col_idx = 0
        for key_metric in selected_metrics_keys:
            if key_metric in current_data:
                value = current_data[key_metric]; label = ALL_CURRENT_METRICS[key_metric]["label"]; unit = ALL_CURRENT_METRICS[key_metric]["unit"]
                format_func = ALL_CURRENT_METRICS[key_metric].get("format_func")
                display_value = f"{value}{unit}" if not format_func else format_func(value)
                with cols[col_idx % num_columns]: st.metric(label=label, value=display_value)
                col_idx += 1
            else: st.warning(f"Data for '{ALL_CURRENT_METRICS[key_metric]['label']}' not available.")
    else: st.warning("Current weather data not available in session state.")

    # 5-Day Forecast (still from current onecall)
    st.header("5-Day Forecast")
    if 'daily' in st.session_state.weather_data and isinstance(st.session_state.weather_data['daily'], list) and len(st.session_state.weather_data['daily']) > 1:
        forecast_table = []
        for day in st.session_state.weather_data['daily'][1:6]: # Slicing for next 5 days
            date = datetime.fromtimestamp(day['dt']).strftime('%A, %B %d')
            max_temp, min_temp = day['temp']['max'], day['temp']['min']
            description = day['weather'][0]['description'].capitalize()
            forecast_table.append([date, f"{max_temp}°C", f"{min_temp}°C", description])
        st.table(forecast_table)
    else: st.warning("Daily forecast data not available or malformed.")

elif st.session_state.weather_type_session == "Historical Weather" and st.session_state.historical_temps is not None:
    st.header("Historical Temperature Trend")
    st.dataframe(st.session_state.historical_temps)

    chart = alt.Chart(st.session_state.historical_temps).mark_line(point=True).encode(
        x=alt.X('Date:T', axis=alt.Axis(format="%b %d")),
        y=alt.Y('Average Temperature (°C):Q', title="Average Temperature (°C)"),
        tooltip=['Date', 'Average Temperature (°C)']
    ).properties(
        title=f"Average Daily Temperature for {st.session_state.location_display.split(': ')[1] if st.session_state.location_display and ':' in st.session_state.location_display else 'Selected Location'}"
    ).interactive()

    st.altair_chart(chart, use_container_width=True)
elif st.session_state.weather_type_session == "Historical Weather" and st.session_state.historical_temps is None:
    st.info("No historical data to display. Please fetch data first.")


# # --- View Past Queries Section (unchanged in functionality, adjusted for session state) ---
# st.markdown("---")
# st.header("View Past Queries")

# if st.button("Show All Past Queries", key="show_past_queries_button"):
#     all_queries = db_cache.get_all_user_queries()
#     if all_queries:
#         all_queries_sorted = sorted(all_queries, key=lambda q: q['query_ts'], reverse=True)
#         query_options = [
#             f"[{datetime.fromtimestamp(q['query_ts']).strftime('%Y-%m-%d %H:%M')}] {q['location_string']} ({'Historical' if q['start_date'] else 'Current'})"
#             for q in all_queries_sorted
#         ]
        
#         selected_query_index = st.selectbox("Select a past query to view:", range(len(query_options)), format_func=lambda x: query_options[x], key="past_query_select")

#         if selected_query_index is not None and st.button("Load Selected Query", key="load_selected_query_button"):
#             # Everything from original Line 352 to 386 moves here
#             selected_query = all_queries_sorted[selected_query_index]
#             location_str = selected_query['location_string']
#             query_start_ts = selected_query['start_date']
#             query_end_ts = selected_query['end_date']

#             st.info(f"Retrieving cached data for: {location_str}")
            
#             st.session_state.weather_data = None
#             st.session_state.historical_temps = None
#             st.session_state.location_display = None
            
#             derived_location_type = "City Name"
#             query_lat, query_lon = None, None
#             if "Lat=" in location_str and "Lon=" in location_str:
#                 derived_location_type = "GPS Coordinates"
#                 try:
#                     parts = location_str.split(': ')[1].split(', ')
#                     query_lat = float(parts[0].replace('Lat=', ''))
#                     query_lon = float(parts[1].replace('Lon=', ''))
#                 except (IndexError, ValueError):
#                     st.warning("Could not parse Lat/Lon from saved GPS string. Attempting lookup by location name.")

#             query_start_date_obj = datetime.fromtimestamp(query_start_ts).date() if query_start_ts else None
#             query_end_date_obj = datetime.fromtimestamp(query_end_ts).date() if query_end_ts else None

#             derived_weather_type = "Historical Weather" if query_start_ts else "Current Weather"
#             st.session_state.weather_type_session = derived_weather_type

#             fetched_weather_data, fetched_historical_temps, fetched_location_display, error_msg = \
#                 _get_weather_data_cached(derived_location_type, location_str, query_lat, query_lon,
#                                         derived_weather_type, query_start_date_obj, query_end_date_obj, API_KEY)
            
#             if error_msg:
#                 st.error(f"Error retrieving past query data: {error_msg}")
#             else:
#                 st.session_state.weather_data = fetched_weather_data
#                 st.session_state.historical_temps = fetched_historical_temps
#                 st.session_state.location_display = fetched_location_display
            
#             st.rerun()
#     else:
#         st.info("No past queries found.")


# --- Database Management Section (unchanged) ---
st.markdown("---")
st.header("Database Management")

# Define editable fields per table
EDITABLE_FIELDS_PER_TABLE = {
    'user_queries': ['location_string'],
    'weather_cache': ['data']
}
# Define preferred display order for specific columns across tables
FIXED_DISPLAY_ORDER_PREFIX = ['lat', 'lon', 'data_ts', 'fetch_ts', 'loc']

with st.expander("Browse and Edit Database Data"):
    db_tables = db_cache.get_table_names()
    if not db_tables:
        st.warning("No database tables found.")
    else:
        # Initialize selected_db_table_view if it's not set or invalid
        if 'selected_db_table_view' not in st.session_state or st.session_state.selected_db_table_view not in db_tables:
            st.session_state.selected_db_table_view = db_tables[0] if db_tables else None

        st.session_state.selected_db_table_view = st.selectbox(
            "Select a Table", db_tables, 
            index=db_tables.index(st.session_state.selected_db_table_view) if st.session_state.selected_db_table_view in db_tables else 0,
            key="db_table_select_management"
        )
        selected_db_table = st.session_state.selected_db_table_view

        if selected_db_table:
            db_columns = db_cache.get_table_columns(selected_db_table)
            pk_cols = db_cache.get_table_primary_key_columns(selected_db_table)

            ordered_db_columns = []
            remaining_columns = []
            
            for col in FIXED_DISPLAY_ORDER_PREFIX:
                if col in db_columns:
                    ordered_db_columns.append(col)
            
            for col in db_columns:
                if col not in ordered_db_columns:
                    remaining_columns.append(col)
            
            ordered_db_columns.extend(sorted(remaining_columns))

            col_sort1, col_sort2 = st.columns(2)
            with col_sort1:
                order_by_col = st.selectbox("Order by Column", [""] + ordered_db_columns, key="order_by_col_select")
            with col_sort2:
                order_direction = st.radio("Direction", ("ASC", "DESC"), key="order_direction_radio")

            data = db_cache.get_table_data(selected_db_table, order_by_col if order_by_col else None, order_direction)
            if data:
                df = pd.DataFrame(data)
                
                if 'data' in df.columns:
                    df['data'] = df['data'].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
                
                st.dataframe(df, use_container_width=True)

                st.subheader("Edit Record")
                st.warning("Directly editing database records can lead to data corruption if not careful. Only specific fields are editable.")
                
                if not pk_cols:
                    st.error(f"Cannot edit records in '{selected_db_table}' as it has no primary key defined.")
                else:
                    st.info(f"To edit a record, you must provide the exact values for its primary key: {', '.join(pk_cols)}")
                    
                    editable_fields_for_this_table = EDITABLE_FIELDS_PER_TABLE.get(selected_db_table, [])
                    
                    if not editable_fields_for_this_table:
                        st.info("No editable fields available for this table.")
                    else:
                        edit_col1, edit_col2 = st.columns(2)
                        with edit_col1:
                            field_to_edit = st.selectbox("Select Field to Update", editable_fields_for_this_table, key="field_to_edit_select")
                        
                        new_value_input = st.text_area(f"Enter new value for '{field_to_edit}'", height=100, key="new_value_input")

                        pk_input_values = {}
                        for pk_col in pk_cols:
                            pk_input_values[pk_col] = st.text_input(f"Value for PK: '{pk_col}'", key=f"pk_input_edit_{pk_col}")

                        if st.button("Update Record", key="update_record_button_db_mgmt"):
                            if not field_to_edit:
                                st.warning("Please select a field to update.")
                                st.stop()
                            
                            try:
                                typed_pk_values = {}
                                pk_complete = True
                                for pk_col in pk_cols:
                                    val_str = pk_input_values[pk_col]
                                    if not val_str:
                                        pk_complete = False
                                        break
                                    if pk_col in ['lat', 'lon']: typed_pk_values[pk_col] = float(val_str)
                                    elif pk_col in ['data_ts', 'query_ts', 'start_date', 'end_date', 'fetch_ts']: typed_pk_values[pk_col] = int(val_str)
                                    else: typed_pk_values[pk_col] = val_str
                                
                                if not pk_complete:
                                    st.error("Please provide valid values for all primary key columns to update a record.")
                                    st.stop()

                                final_new_value = new_value_input
                                if field_to_edit == 'data':
                                    final_new_value = json.loads(new_value_input)
                                db_cache.update_record(selected_db_table, typed_pk_values, field_to_edit, final_new_value)
                                st.success(f"Record updated successfully in '{selected_db_table}'. Refreshing data...")
                                # Invalidate cache if weather_cache is updated directly
                                if selected_db_table == 'weather_cache':
                                    _get_weather_data_cached.clear() # Clear the cache for fresh data
                                st.rerun()
                            except ValueError as ve:
                                st.error(f"Data type error: {ve}. Please ensure values match expected types (e.g., numbers, integers, valid JSON for 'data').")
                            except json.JSONDecodeError:
                                st.error("Invalid JSON format for 'data' field. Please enter a valid JSON string.")
                            except Exception as e:
                                st.error(f"Error updating record: {e}")
                
                st.subheader("Delete Record")
                st.warning("Deleting records is irreversible. Confirm carefully.")
                
                delete_pk_input_values = {}
                for pk_col in pk_cols:
                    delete_pk_input_values[pk_col] = st.text_input(f"Value for PK to delete: '{pk_col}'", key=f"delete_pk_input_{pk_col}")

                if st.button("Delete Record", key="delete_record_button_db_mgmt"):
                    try:
                        typed_delete_pk_values = {}
                        delete_pk_complete = True
                        for pk_col in pk_cols:
                            val_str = delete_pk_input_values[pk_col]
                            if not val_str:
                                delete_pk_complete = False
                                break
                            if pk_col in ['lat', 'lon']: typed_delete_pk_values[pk_col] = float(val_str)
                            elif pk_col in ['data_ts', 'query_ts', 'start_date', 'end_date', 'fetch_ts']: typed_delete_pk_values[pk_col] = int(val_str)
                            else: typed_delete_pk_values[pk_col] = val_str
                        
                        if not delete_pk_complete:
                            st.error("Please provide valid values for all primary key columns to delete a record.")
                            st.stop()

                        db_cache.delete_record(selected_db_table, typed_delete_pk_values)
                        st.success(f"Record deleted successfully from '{selected_db_table}'. Refreshing data...")
                        # Invalidate cache if weather_cache is deleted from
                        if selected_db_table == 'weather_cache':
                            _get_weather_data_cached.clear() # Clear the cache for fresh data
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error deleting record: {e}")
            else:
                st.info("No data found in this table.")