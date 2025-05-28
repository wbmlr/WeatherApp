# db_cache.py
import pymysql
import json
import time
import os
from datetime import datetime, date, timedelta # Import timedelta for date range iteration

from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

CACHE_DURATION_SECONDS = 43200 # 12 hrs

def get_db_connection():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

def init_db():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            table_name = "weather_cache"

            create_weather_cache_sql = f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    lat REAL NOT NULL,
                    lon REAL NOT NULL,
                    data_ts INTEGER NOT NULL,
                    fetch_ts INTEGER NOT NULL,
                    loc VARCHAR(255),
                    data TEXT,
                    PRIMARY KEY (lat, lon, data_ts)
                )
            '''
            cursor.execute(create_weather_cache_sql)
            conn.commit()

            try:
                cursor.execute(f"SELECT data_ts FROM {table_name} LIMIT 1")
            except pymysql.err.ProgrammingError:
                print(f"Adding 'data_ts' column to {table_name}...")
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN data_ts INTEGER;")
                conn.commit()

            try:
                cursor.execute(f"SELECT fetch_ts FROM {table_name} LIMIT 1")
            except pymysql.err.ProgrammingError:
                print(f"Adding 'fetch_ts' column to {table_name}...")
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN fetch_ts INTEGER;")
                conn.commit()

            print(f"Updating NULL data_ts values in {table_name}...")
            cursor.execute(f"UPDATE {table_name} SET data_ts = 0 WHERE data_ts IS NULL;")
            conn.commit()

            print(f"Updating NULL fetch_ts values in {table_name}...")
            cursor.execute(f"UPDATE {table_name} SET fetch_ts = %s WHERE fetch_ts IS NULL;", (int(time.time()),))
            conn.commit()

            try:
                print(f"Setting data_ts and fetch_ts to NOT NULL in {table_name}...")
                cursor.execute(f"ALTER TABLE {table_name} MODIFY COLUMN data_ts INTEGER NOT NULL;")
                cursor.execute(f"ALTER TABLE {table_name} MODIFY COLUMN fetch_ts INTEGER NOT NULL;")
                conn.commit()
            except pymysql.err.OperationalError as e:
                if "Invalid use of NULL value" in str(e):
                    print(f"WARNING: 'data_ts' or 'fetch_ts' still has NULLs despite UPDATE. Please manually verify table data. Error: {e}")
                elif "Duplicate column name" not in str(e):
                    raise
            
            cursor.execute(f"""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = '{DB_NAME}' AND TABLE_NAME = '{table_name}' AND CONSTRAINT_NAME = 'PRIMARY'
                ORDER BY ORDINAL_POSITION;
            """)
            current_pk_columns = [row['COLUMN_NAME'] for row in cursor.fetchall()]
            required_pk_columns = ['lat', 'lon', 'data_ts']

            if sorted(current_pk_columns) != sorted(required_pk_columns):
                print(f"Primary key needs update for {table_name}. Current: {current_pk_columns}, Required: {required_pk_columns}")
                if current_pk_columns:
                    print(f"Dropping existing primary key from {table_name}...")
                    try:
                        cursor.execute(f"ALTER TABLE {table_name} DROP PRIMARY KEY;")
                        conn.commit()
                    except pymysql.err.OperationalError as drop_e:
                        print(f"WARNING: Failed to drop existing primary key. Error: {drop_e}")
                
                print(f"Adding new primary key (lat, lon, data_ts) to {table_name}...")
                try:
                    cursor.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY (lat, lon, data_ts);")
                    conn.commit()
                except pymysql.err.OperationalError as add_pk_e:
                    print(f"CRITICAL: Failed to add primary key (lat, lon, data_ts) to {table_name}. Error: {add_pk_e}")
                    print("This usually means duplicate (lat, lon, data_ts) entries exist in your data, or NULL values are still present.")
                    raise
            else:
                print(f"Primary key for {table_name} is already correct ({current_pk_columns}).")

            create_user_queries_sql = '''
                CREATE TABLE IF NOT EXISTS user_queries (
                    session_id VARCHAR(255) NOT NULL,
                    query_ts INTEGER NOT NULL,
                    location_string VARCHAR(255) NOT NULL,
                    start_date INTEGER,
                    end_date INTEGER,
                    PRIMARY KEY (session_id, query_ts)
                )
            '''
            cursor.execute(create_user_queries_sql)
            conn.commit()

def get_cache(lat=None, lon=None, target_data_ts=None, location=None):
    """
    Retrieves weather data from cache.
    target_data_ts: If None, fetches the latest fresh current weather. If provided, fetches specific historical data.
    location: Used as a fallback to find lat/lon if lat/lon are not provided, primarily for current weather or to aid historical lookup.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            # If lat and lon are not provided, try to find a recent entry by location string to get lat/lon first
            if (lat is None or lon is None) and location:
                cursor.execute("""
                    SELECT lat, lon FROM weather_cache
                    WHERE loc = %s
                    ORDER BY fetch_ts DESC
                    LIMIT 1
                """, (location,))
                coords_row = cursor.fetchone()
                if coords_row:
                    lat, lon = coords_row['lat'], coords_row['lon']
                else:
                    return None

            if lat is None or lon is None: # If still no lat/lon, cannot proceed
                return None

            if target_data_ts is None: # Fetch latest current weather by lat/lon
                cursor.execute("""
                    SELECT data FROM weather_cache
                    WHERE lat = %s AND lon = %s AND fetch_ts > %s
                    ORDER BY fetch_ts DESC
                    LIMIT 1
                """, (lat, lon, int(time.time()) - CACHE_DURATION_SECONDS))
            else: # Fetch specific historical data by lat/lon and data_ts
                cursor.execute("""
                    SELECT data FROM weather_cache
                    WHERE lat = %s AND lon = %s AND data_ts = %s
                    LIMIT 1
                """, (lat, lon, target_data_ts))
            
            row = cursor.fetchone()
    return json.loads(row['data']) if row else None

def get_cache_for_range(lat, lon, start_date_ts, end_date_ts):
    """
    Retrieves historical weather data from cache for a given lat/lon and date range.
    Returns a dictionary mapping data_ts to fetched data.
    """
    cached_data = {}
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            # Fetch all records within the timestamp range for the given lat/lon
            cursor.execute("""
                SELECT data_ts, data FROM weather_cache
                WHERE lat = %s AND lon = %s AND data_ts BETWEEN %s AND %s
            """, (lat, lon, start_date_ts, end_date_ts))
            rows = cursor.fetchall()
            for row in rows:
                cached_data[row['data_ts']] = json.loads(row['data'])
    return cached_data

def set_cache(lat, lon, location, data, data_ts):
    """Stores weather data in cache."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                REPLACE INTO weather_cache (lat, lon, loc, data_ts, fetch_ts, data)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (lat, lon, location, data_ts, int(time.time()), json.dumps(data)))
        conn.commit()

def log_user_query(session_id, location_string, start_date_ts=None, end_date_ts=None):
    """Logs a user's weather query."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO user_queries (session_id, query_ts, location_string, start_date, end_date)
                VALUES (%s, %s, %s, %s, %s)
            """, (session_id, int(time.time()), location_string, start_date_ts, end_date_ts))
        conn.commit()

def get_all_user_queries():
    """Retrieves all user queries."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT session_id, query_ts, location_string, start_date, end_date FROM user_queries ORDER BY query_ts DESC")
            return cursor.fetchall()

def get_table_names():
    """Retrieves all table names in the database."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SHOW TABLES;")
            # When using DictCursor, 'SHOW TABLES' returns a dictionary
            # with a key like 'Tables_in_<DB_NAME>'.
            return [row['Tables_in_' + DB_NAME] for row in cursor.fetchall()]

def get_table_columns(table_name):
    """Retrieves column names for a given table."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"DESCRIBE `{table_name}`;")
            return [row['Field'] for row in cursor.fetchall()]

def get_table_primary_key_columns(table_name):
    """Retrieves primary key column names for a given table."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = '{DB_NAME}' AND TABLE_NAME = '{table_name}' AND CONSTRAINT_NAME = 'PRIMARY'
                ORDER BY ORDINAL_POSITION;
            """)
            return [row['COLUMN_NAME'] for row in cursor.fetchall()]

def get_table_data(table_name, order_by_column=None, order_direction='ASC'):
    """Retrieves all data from a specified table with optional sorting."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            query = f"SELECT * FROM `{table_name}`"
            if order_by_column and order_by_column in get_table_columns(table_name):
                query += f" ORDER BY `{order_by_column}` {order_direction}"
            cursor.execute(query)
            return cursor.fetchall()

def update_record(table_name, pk_dict, field_to_update, new_value):
    """Updates a specific field in a record identified by its primary key."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            pk_conditions = []
            pk_values = []
            for pk_col, pk_val in pk_dict.items():
                pk_conditions.append(f"`{pk_col}` = %s")
                pk_values.append(pk_val)
            where_clause = " AND ".join(pk_conditions)

            final_value_for_db = new_value
            if field_to_update == 'data':
                final_value_for_db = json.dumps(new_value) 
            
            sql = f"UPDATE `{table_name}` SET `{field_to_update}` = %s WHERE {where_clause}"
            params = [final_value_for_db] + pk_values
            cursor.execute(sql, params)
            conn.commit()

def delete_record(table_name, pk_dict):
    """Deletes a record from a specified table identified by its primary key."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            pk_conditions = [f"`{pk_col}` = %s" for pk_col in pk_dict.keys()]
            where_clause = " AND ".join(pk_conditions)
            sql = f"DELETE FROM `{table_name}` WHERE {where_clause}"
            params = list(pk_dict.values())
            cursor.execute(sql, params)
            conn.commit()
