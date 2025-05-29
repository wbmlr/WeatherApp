# db_cache.py
import sqlite3
import json
import time
import os
from datetime import datetime

# Use a local file for the SQLite database
DB_NAME = "local_weather_cache.db"
CACHE_DURATION_SECONDS = 43200  # 12 hours

def get_db_connection():
    """Establishes a connection to the SQLite database."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row  # Access columns by name
    return conn

def init_db():
    """Initializes the database tables if they don't exist."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        # weather_cache table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS weather_cache (
                lat REAL NOT NULL,
                lon REAL NOT NULL,
                data_ts INTEGER NOT NULL,
                fetch_ts INTEGER NOT NULL,
                loc TEXT,
                data TEXT,
                PRIMARY KEY (lat, lon, data_ts)
            )
        ''')
        # user_queries table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_queries (
                session_id TEXT NOT NULL,
                query_ts INTEGER NOT NULL,
                location_string TEXT NOT NULL,
                start_date INTEGER,
                end_date INTEGER,
                PRIMARY KEY (session_id, query_ts)
            )
        ''')
        conn.commit()
    print(f"SQLite database '{DB_NAME}' initialized/checked.")

def get_cache(lat=None, lon=None, target_data_ts=None, location=None):
    """Retrieves weather data from SQLite cache."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        if (lat is None or lon is None) and location:
            cursor.execute("SELECT lat, lon FROM weather_cache WHERE loc = ? ORDER BY fetch_ts DESC LIMIT 1", (location,))
            coords_row = cursor.fetchone()
            if coords_row:
                lat, lon = coords_row['lat'], coords_row['lon']
            else:
                return None
        if lat is None or lon is None:
            return None

        if target_data_ts is None:  # Fetch latest current weather
            cursor.execute("""
                SELECT data FROM weather_cache
                WHERE lat = ? AND lon = ? AND fetch_ts > ?
                ORDER BY fetch_ts DESC
                LIMIT 1
            """, (lat, lon, int(time.time()) - CACHE_DURATION_SECONDS))
        else:  # Fetch specific historical data
            cursor.execute("""
                SELECT data FROM weather_cache
                WHERE lat = ? AND lon = ? AND data_ts = ?
                LIMIT 1
            """, (lat, lon, target_data_ts))
        row = cursor.fetchone()
    return json.loads(row['data']) if row and row['data'] else None

def get_cache_for_range(lat, lon, start_date_ts, end_date_ts):
    """Retrieves historical weather data from SQLite cache for a date range."""
    cached_data = {}
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT data_ts, data FROM weather_cache
            WHERE lat = ? AND lon = ? AND data_ts BETWEEN ? AND ?
        """, (lat, lon, start_date_ts, end_date_ts))
        for row in cursor.fetchall():
            if row['data']:
                cached_data[row['data_ts']] = json.loads(row['data'])
    return cached_data

def set_cache(lat, lon, location, data, data_ts):
    """Stores weather data in SQLite cache."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            REPLACE INTO weather_cache (lat, lon, loc, data_ts, fetch_ts, data)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (lat, lon, location, data_ts, int(time.time()), json.dumps(data)))
        conn.commit()

def log_user_query(session_id, location_string, start_date_ts=None, end_date_ts=None):
    """Logs user query details into the SQLite database."""
    query_ts = int(datetime.now().timestamp())
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO user_queries (session_id, query_ts, location_string, start_date, end_date)
            VALUES (?, ?, ?, ?, ?)
        """, (session_id, query_ts, location_string, start_date_ts, end_date_ts))
        conn.commit()

def get_all_user_queries():
    """Retrieves all user queries from SQLite, ordered by query time."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT session_id, query_ts, location_string, start_date, end_date FROM user_queries ORDER BY query_ts DESC")
        return [dict(row) for row in cursor.fetchall()] # Convert rows to dicts

def get_table_names():
    """Retrieves all table names in the SQLite database."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        return [row['name'] for row in cursor.fetchall()]

def get_table_columns(table_name):
    """Retrieves column names for a given table in SQLite."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info(`{table_name}`);")
        return [row['name'] for row in cursor.fetchall()]

def get_table_primary_key_columns(table_name):
    """Retrieves primary key column names for a given table in SQLite."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info(`{table_name}`);")
        return [row['name'] for row in cursor.fetchall() if row['pk'] > 0]

def get_table_data(table_name, order_by_column=None, order_direction='ASC'):
    """Retrieves all data from a specified table in SQLite with optional sorting."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        query = f"SELECT * FROM `{table_name}`"
        if order_by_column and order_by_column in get_table_columns(table_name):
            # Ensure order_direction is safe if it's user-provided elsewhere, though here it's fixed
            direction = "ASC" if order_direction.upper() == "ASC" else "DESC"
            query += f" ORDER BY `{order_by_column}` {direction}"
        cursor.execute(query)
        return [dict(row) for row in cursor.fetchall()] # Convert rows to dicts

def update_record(table_name, pk_dict, field_to_update, new_value):
    """Updates a specific field in a record in SQLite, identified by its primary key."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        pk_conditions = []
        pk_values = []
        for pk_col, pk_val in pk_dict.items():
            pk_conditions.append(f"`{pk_col}` = ?")
            pk_values.append(pk_val)
        where_clause = " AND ".join(pk_conditions)

        final_value_for_db = json.dumps(new_value) if field_to_update == 'data' else new_value

        sql = f"UPDATE `{table_name}` SET `{field_to_update}` = ? WHERE {where_clause}"
        params = [final_value_for_db] + pk_values
        cursor.execute(sql, params)
        conn.commit()

def delete_record(table_name, pk_dict):
    """Deletes a record from a specified table in SQLite, identified by its primary key."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        pk_conditions = [f"`{pk_col}` = ?" for pk_col in pk_dict.keys()]
        where_clause = " AND ".join(pk_conditions)
        sql = f"DELETE FROM `{table_name}` WHERE {where_clause}"
        params = list(pk_dict.values())
        cursor.execute(sql, params)
        conn.commit()

if __name__ == '__main__':
    # Example usage (optional, for testing)
    print("Running db_cache.py standalone for testing.")
    init_db()
    print("Tables:", get_table_names())

    # Test set_cache and get_cache
    test_lat, test_lon = 10.0, 20.0
    test_data_ts = int(time.time())
    test_loc = "TestLocation"
    test_weather_data = {"temp": 25, "desc": "sunny"}
    set_cache(test_lat, test_lon, test_loc, test_weather_data, test_data_ts)
    print("Cache set for TestLocation.")

    retrieved_data = get_cache(lat=test_lat, lon=test_lon, target_data_ts=test_data_ts)
    print("Retrieved from cache:", retrieved_data)
    assert retrieved_data == test_weather_data

    # Test log_user_query and get_all_user_queries
    log_user_query("test_session_123", "TestCitySearch")
    queries = get_all_user_queries()
    print("All user queries:", queries)
    assert len(queries) > 0

    # Test table metadata functions
    if 'weather_cache' in get_table_names():
        print("Weather_cache columns:", get_table_columns('weather_cache'))
        print("Weather_cache PK:", get_table_primary_key_columns('weather_cache'))
        print("Weather_cache data:", get_table_data('weather_cache'))

    print("DB cache tests completed.")
