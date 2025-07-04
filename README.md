# 🌦️ Weather ETL Pipeline using Apache Airflow

This project implements an **ETL pipeline using Apache Airflow** to extract, transform, and load real-time weather data from the [Open-Meteo API](https://open-meteo.com/). The pipeline collects weather data such as temperature and wind information for a specific geographic location and stores it in a PostgreSQL database.

---



## 🚀 Features

- **Data Extraction**: Pulls current weather data from Open-Meteo API
- **Data Transformation**: Extracts relevant fields (temperature, wind, time)
- **Data Loading**: Inserts transformed data into a PostgreSQL table
- **Airflow UI**: View DAGs, monitor runs, and manage tasks via web UI

---

## 🧠 DAG Overview

| Task | Description |
|------|-------------|
| `extract_weather_data` | Uses `HttpHook` to request weather data from Open-Meteo |
| `transform_weather_data` | Transforms JSON into a structured dictionary |
| `load_weather_data` | Loads data into a PostgreSQL `weather_data` table |

---

## ⚙️ Technologies

- [Apache Airflow](https://airflow.apache.org/)
- [Open-Meteo API](https://open-meteo.com/)
- PostgreSQL
- Astro CLI (via [Astronomer](https://www.astronomer.io/))

---

## 📦 Installation & Setup

1. **Install Astro CLI** (if not done already):  
   [Install instructions](https://docs.astronomer.io/astro/cli/install-cli)

2. **Clone this repo**:

   ```bash
   git clone https://github.com/your-username/weather-etl-airflow.git
   cd weather-etl-airflow
Add dependencies to requirements.txt:

text
Copy
Edit
apache-airflow
apache-airflow-providers-http
apache-airflow-providers-postgres
pandas
requests
Start Airflow locally:

astro dev start
Open the Airflow UI:
Visit http://localhost:8080

🧪 How to Run the DAG
Turn on the weather_etl_pipeline toggle in the UI

Click the Trigger DAG button to run it manually

Monitor task execution via the Graph View or Logs

🗃️ PostgreSQL Table Schema
sql
Copy
Edit
CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    latitude FLOAT,
    longitude FLOAT,
    temperature FLOAT,
    winddirection FLOAT,
    windspeed FLOAT,
    weathercode INT,
    time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
📌 Future Enhancements
Add historical weather tracking

Send data to cloud storage (e.g., AWS S3 or Google BigQuery)

Add data validation and alerting

Parameterize location inputs via Airflow Variables

🧑‍💻 Author
Kaverappa (@kaverapp-soul1)
Powered by Astronomer & Airflow ☁️

📜 License
MIT License. Use freely and responsibly.

