from airflow import DAG
from airflow.providers.https.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago

LATITUDE="51.5074"
LONGITUDE="-0.1278"
POSTGRES_CON_ID="postgres_default"
API_CON_ID="open_meteorology_api"

default_args={
    "owner":"airflow",
    "start_date":days_ago(1)
}

with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:
    def extract_weather_data():
        """Extract weather data from Open-Meteo API using Airflow"""

        #use http Hook to get connection details from Airfow connection
        http_hook=HttpHook(http_conn_id=API_CON_ID,method="GET")

        ## Build the API endpoint URL
        endpoint=f"/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true"
        
# https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m
        
        # Make the API request
        response=http_hook.run(endpoint)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch data: {response.text}")
        return response.json()
    
    @task
    def transform_weather_data(weather_data):
        """Transform the weather data to extract relevant fields"""
        current_weather = weather_data["current_weather"]
        transformed_data = {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "temperature": current_weather.get("temperature", None),
            "winddirection": current_weather.get("winddirection", None),
            "windspeed": current_weather.get("windspeed", None),
            "time": current_weather.get("time", None)
        }
        return transformed_data
    
    @task
    def load_weather_data(transformed_data):
        """Load the transformed data into PostgreSQL"""
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CON_ID)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
       
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            id SERIAL PRIMARY KEY,
            latitude FLOAT,
            longitude FLOAT ,
            temperature FLOAT,
            winddirection FLOAT,
            windspeed FLOAT,
            weathercode INT,
            time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
        
        insert_query = """
        INSERT INTO weather_data (latitude, longitude, temperature, winddirection, windspeed, time)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            transformed_data["latitude"],
            transformed_data["longitude"],
            transformed_data["temperature"],
            transformed_data["winddirection"],
            transformed_data["windspeed"],
            transformed_data["time"]
        ))
        
        conn.commit()
        cursor.close()

    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)
   