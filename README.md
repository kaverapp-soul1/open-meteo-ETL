# Airflow Weather ETL Pipeline

A simple Extract, Transform, Load (ETL) pipeline using Apache Airflow to fetch current weather data from the Open-Meteo API and store it in a PostgreSQL database.

## 🌟 Features

- **Extract**: Fetches real-time weather data (temperature, wind direction, wind speed, weather code, time) for a specified latitude and longitude using the Open-Meteo API
- **Transform**: Processes the raw JSON response, extracting only the relevant current weather attributes
- **Load**: Inserts the transformed weather data into a PostgreSQL database table, creating the table if it doesn't already exist
- **Airflow Orchestration**: Leverages Airflow's DAGs, @task decorator, HttpHook, and PostgresHook for robust and scheduled execution

## 🚀 Getting Started

Follow these steps to set up and run the ETL pipeline:

### 📋 Prerequisites

1. **Apache Airflow**: Ensure you have Apache Airflow installed and running. This DAG is compatible with Airflow 2.x (specifically, it uses `schedule` instead of `schedule_interval` and the `@task` decorator).

2. **Provider Packages**: Install the necessary Airflow provider packages:
   ```bash
   pip install apache-airflow-providers-http
   pip install apache-airflow-providers-postgres
   ```

3. **PostgreSQL Database**: Ensure you have access to a PostgreSQL database where weather data will be stored.

### 🔗 Airflow Connections Setup

This DAG relies on two Airflow Connections to interact with the external API and the PostgreSQL database.

#### Open-Meteo API Connection

1. Go to the Airflow UI
2. Navigate to **Admin** → **Connections**
3. Click the **+** button to create a new connection
4. Configure the connection as follows:
   - **Conn Id**: `open_meteorology_api` (This must match the `API_CON_ID` in the DAG file)
   - **Conn Type**: HTTP
   - **Host**: `api.open-meteo.com`
   - **Schema**: `https`
   - **Port**: (Leave empty, or 443 for HTTPS)
   - **Login**: (Leave empty)
   - **Password**: (Leave empty)
   - **Extra**: (Leave empty)
5. Click **Save**

#### PostgreSQL Database Connection

1. Go to the Airflow UI
2. Navigate to **Admin** → **Connections**
3. Click the **+** button to create a new connection
4. Configure the connection as follows:
   - **Conn Id**: `postgres_default` (This must match the `POSTGRES_CON_ID` in the DAG file)
   - **Conn Type**: Postgres
   - **Host**: Your PostgreSQL host (e.g., `localhost` or the IP address of your DB server)
   - **Schema**: Your database name (e.g., `airflow_db` or `weather_data_db`)
   - **Login**: Your PostgreSQL username
   - **Password**: Your PostgreSQL password
   - **Port**: Your PostgreSQL port (default is 5432)
   - **Extra**: (Leave empty, or add specific sslmode if needed for your setup)
5. Click **Save**

### 📁 Installation

1. **Place the DAG file**: Save the `etlweather.py` file in your Airflow `dags` folder
2. **Configure location**: Update the `LATITUDE` and `LONGITUDE` constants in the DAG file to your desired location (currently set to London)

## 🏃 How to Run

### Enable the DAG
1. Airflow should automatically detect the new DAG
2. Go to the Airflow UI
3. Find `weather_etl_pipeline` in the DAGs list
4. Toggle it **On**

### Trigger the DAG

**Manually:**
1. Click on the `weather_etl_pipeline` DAG in the UI
2. Click **Trigger DAG**

**Scheduled:**
- The DAG is scheduled to run daily (`@daily`) starting from 2025-10-01
- The Airflow scheduler will pick it up automatically based on this schedule

## 📊 Database Schema

The `load_weather_data` task will create a table named `weather_data` in your PostgreSQL database (if it doesn't already exist) with the following schema:

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `id` | SERIAL PRIMARY KEY | Unique identifier for each record |
| `latitude` | FLOAT | Latitude of the weather data point |
| `longitude` | FLOAT | Longitude of the weather data point |
| `temperature` | FLOAT | Current temperature in Celsius |
| `winddirection` | FLOAT | Wind direction in degrees |
| `windspeed` | FLOAT | Wind speed in km/h |
| `weathercode` | INT | WMO Weather interpretation code |
| `time` | TIMESTAMP WITH TIME ZONE | Timestamp of the weather observation (UTC) |

## 🏗️ Pipeline Architecture

The ETL pipeline consists of three main tasks:

1. **Extract Weather Data**: Fetches current weather data from the Open-Meteo API using Airflow's HttpHook
2. **Transform Weather Data**: Processes the raw JSON response and extracts relevant fields
3. **Load Weather Data**: Inserts the transformed data into PostgreSQL using Airflow's PostgresHook

## 🔧 Configuration

### Location Configuration
Update the following constants in `etlweather.py` to change the weather data location:

```python
LATITUDE = "51.5074"  # Example: London's Latitude
LONGITUDE = "-0.1278" # Example: London's Longitude
```

### Schedule Configuration
The DAG runs daily by default. To change the schedule, modify the `schedule` parameter:

```python
schedule="@daily"  # Options: @hourly, @daily, @weekly, @monthly, or cron expression
```

## 💡 Future Enhancements

- **Error Handling & Alerting**: Implement more sophisticated error handling and integrate with alerting systems (e.g., Slack, email) for failed tasks
- **Idempotency**: Ensure the `load_weather_data` task is fully idempotent to prevent duplicate records if a DAG run is re-executed
- **Historical Data**: Modify the extract task to fetch historical weather data for a given date range
- **Dynamic Locations**: Allow latitude and longitude to be passed as DAG parameters or fetched from a configuration source
- **Data Quality Checks**: Add tasks to perform data quality checks on the extracted and transformed data before loading
- **Data Visualization**: Integrate with a data visualization tool (e.g., Superset, Grafana) to display the collected weather data

## 🐛 Troubleshooting

### Common Issues

1. **Connection Errors**: Ensure both Airflow connections are properly configured with correct credentials
2. **Database Permissions**: Verify that the PostgreSQL user has CREATE and INSERT permissions
3. **API Rate Limits**: The Open-Meteo API is free but may have rate limits for excessive requests
4. **Time Zone Issues**: Weather data timestamps are stored in UTC

### Logs and Monitoring

- Check task logs in the Airflow UI under **Browse** → **Task Instances**
- Monitor DAG runs in the **Grid View** or **Graph View**
- Use the **Gantt Chart** to analyze task execution times

## 📄 License

This project is open source and available under the [MIT License](LICENSE).

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📞 Support

For questions or issues, please open an issue in the project repository.
