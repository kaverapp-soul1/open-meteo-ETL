from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime, timedelta # Import timedelta for potential future use or clarity
import requests
import json

# Define constants for latitude, longitude, and connection IDs
LATITUDE = "51.5074"
LONGITUDE = "-0.1278"
POSTGRES_CON_ID = "postgres_default"
API_CON_ID = "open_meteorology_api"

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 7, 4), # Fixed start date is recommended
    "retries": 1, # Add retries for robustness
    "retry_delay": timedelta(minutes=5), # Delay between retries
}

# Define the DAG
with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    # Changed 'schedule_interval' to 'schedule' as per Airflow 2.x updates
    schedule="@daily", # Schedule to run daily
    catchup=False, # Do not backfill past missed runs
    tags=["weather", "etl", "api", "postgres"], # Add relevant tags
    doc_md="""
    ### Weather ETL Pipeline
    This DAG extracts current weather data from the Open-Meteo API,
    transforms it, and loads it into a PostgreSQL database.
    """
) as dag:

    @task
    def extract_weather_data():
        """
        Extracts current weather data from the Open-Meteo API.
        Uses Airflow's HttpHook to leverage pre-configured connections.
        """
        # Initialize HttpHook with the defined API connection ID
        http_hook = HttpHook(http_conn_id=API_CON_ID, method="GET")

        # Build the API endpoint URL for current weather data
        # The 'current_weather=true' parameter is crucial for getting current conditions
        endpoint = f"/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true"

        # Make the API request using the HttpHook
        response = http_hook.run(endpoint)

        # Check if the API request was successful (status code 200)
        if response.status_code != 200:
            # Raise an exception if the request failed, including the response text for debugging
            raise Exception(f"Failed to fetch data from Open-Meteo API: {response.text}")

        # Return the JSON response from the API
        return response.json()

    @task
    def transform_weather_data(weather_data):
        """
        Transforms the raw weather data extracted from the API into a structured format.
        Extracts specific fields like temperature, wind direction, wind speed, weather code, and time.
        """
        # Access the 'current_weather' dictionary from the API response
        current_weather = weather_data.get("current_weather", {})

        # Create a dictionary with the transformed data
        # Using .get() with a default value (None) to prevent KeyError if a field is missing
        transformed_data = {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "temperature": current_weather.get("temperature"),
            "winddirection": current_weather.get("winddirection"),
            "windspeed": current_weather.get("windspeed"),
            "weathercode": current_weather.get("weathercode"), # Include weathercode
            "time": current_weather.get("time") # Time is typically an ISO 8601 string
        }
        return transformed_data

    @task
    def load_weather_data(transformed_data):
        """
        Loads the transformed weather data into a PostgreSQL database.
        Connects to PostgreSQL using Airflow's PostgresHook.
        Creates the 'weather_data' table if it doesn't exist and then inserts the data.
        """
        # Initialize PostgresHook with the defined PostgreSQL connection ID
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CON_ID)
        
        # Get a database connection and cursor
        conn = None # Initialize conn to None
        cursor = None # Initialize cursor to None
        try:
            conn = postgres_hook.get_conn()
            cursor = conn.cursor()

            # Create the weather_data table if it does not already exist
            # Using TIMESTAMP WITH TIME ZONE for better handling of timezone-aware datetimes
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS weather_data (
                    id SERIAL PRIMARY KEY,
                    latitude FLOAT,
                    longitude FLOAT,
                    temperature FLOAT,
                    winddirection FLOAT,
                    windspeed FLOAT,
                    weathercode INT,
                    time TIMESTAMP WITH TIME ZONE
                );
            """)

            # Prepare the INSERT query
            insert_query = """
                INSERT INTO weather_data (latitude, longitude, temperature, winddirection, windspeed, weathercode, time)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

            # Convert latitude and longitude to float to ensure correct type for DB
            lat_float = float(transformed_data["latitude"])
            lon_float = float(transformed_data["longitude"])

            # Parse the time string into a timezone-aware datetime object
            # Open-Meteo provides time in ISO 8601 format, often ending with 'Z' for UTC.
            # datetime.fromisoformat() can handle this, but for 'Z' it expects '+00:00'
            record_time = None
            if transformed_data["time"]:
                try:
                    # Replace 'Z' with '+00:00' for datetime.fromisoformat to parse UTC correctly
                    record_time = datetime.fromisoformat(transformed_data["time"].replace('Z', '+00:00'))
                except (TypeError, ValueError) as e:
                    print(f"Warning: Could not parse time '{transformed_data['time']}'. Using None. Error: {e}")
                    record_time = None

            # Execute the INSERT query with the transformed data
            cursor.execute(insert_query, (
                lat_float,
                lon_float,
                transformed_data["temperature"],
                transformed_data["winddirection"],
                transformed_data["windspeed"],
                transformed_data["weathercode"], # Pass weathercode here
                record_time
            ))

            # Commit the transaction to save changes to the database
            conn.commit()

        except Exception as e:
            # Rollback in case of any error
            if conn:
                conn.rollback()
            raise Exception(f"Failed to load data into PostgreSQL: {e}")
        finally:
            # Ensure cursor and connection are closed
            if cursor:
                cursor.close()
            if conn:
                conn.close()


    # Define the task dependencies
    # The output of extract_weather_data is passed as input to transform_weather_data,
    # and the output of transform_weather_data is passed to load_weather_data.
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)
