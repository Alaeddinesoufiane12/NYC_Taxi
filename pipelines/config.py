# -*- coding: utf-8 -*-
"""Configuration centrale du datalake NYC Taxi : chemins, constantes, environnement Spark."""
import os
import sys
import tempfile
from datetime import date
from pathlib import Path

# Racine du projet = parent du package pipelines (indépendant du cwd)
BASE = Path(__file__).resolve().parents[1]
DATA_DIR = BASE / "data"                  # miroir local des fichiers TLC téléchargés
DATALAKE = BASE / "datalake"
BRONZE = DATALAKE / "bronze"
SILVER = DATALAKE / "silver"
GOLD_DIR = DATALAKE / "gold"
REF_DIR = DATALAKE / "ref"
NOTEBOOKS_DIR = BASE / "notebooks"


def setup_env():
    """Environnement PySpark sous Windows + stdout UTF-8. À appeler avant toute session Spark."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    hadoop_home = Path(r"C:\hadoop")          # winutils.exe + hadoop.dll
    if hadoop_home.exists():
        os.environ["HADOOP_HOME"] = str(hadoop_home)
        os.environ["PATH"] = str(hadoop_home / "bin") + os.pathsep + os.environ.get("PATH", "")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


setup_env()

TLC_TRIP_DATA = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TLC_ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
OPEN_METEO_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"

VEHICLES = {  # type -> (premier mois publié, préfixe de fichier TLC)
    "yellow": ("2009-01", "yellow_tripdata"),
    "green": ("2013-08", "green_tripdata"),
    "fhv": ("2015-01", "fhv_tripdata"),
    "fhvhv": ("2019-02", "fhvhv_tripdata"),
}

HOURLY_VARS = ["temperature_2m", "relative_humidity_2m", "precipitation",
               "rain", "snowfall", "wind_speed_10m", "weather_code"]
NYC_POINT = {"latitude": 40.7128, "longitude": -74.0060,
             "timezone": "America/New_York"}  # Lower Manhattan
WEATHER_VARS_FR = ["temperature_c", "humidite_pct", "precipitation_mm",
                   "pluie_mm", "neige_mm", "vent_kmh", "code_meteo"]

PUBLICATION_LAG_MONTHS = 2   # délai de publication annoncé par la TLC

# Anciens et nouveaux noms de colonnes => schéma canonique (le premier candidat présent gagne)
COLMAP = {
    "pickup_datetime":       ["Trip_Pickup_DateTime", "tpep_pickup_datetime", "lpep_pickup_datetime", "pickup_datetime"],
    "dropoff_datetime":      ["Trip_Dropoff_DateTime", "tpep_dropoff_datetime", "lpep_dropoff_datetime", "dropoff_datetime", "dropOff_datetime"],
    "vendor_id":             ["vendor_name", "VendorID", "hvfhs_license_num", "Dispatching_base_num"],
    "passenger_count":       ["Passenger_Count", "passenger_count"],
    "trip_distance":         ["Trip_Distance", "trip_distance", "trip_miles"],
    "pickup_longitude":      ["Start_Lon", "pickup_longitude"],
    "pickup_latitude":       ["Start_Lat", "pickup_latitude"],
    "dropoff_longitude":     ["End_Lon", "dropoff_longitude"],
    "dropoff_latitude":      ["End_Lat", "dropoff_latitude"],
    "pub_pickup_zone":       ["PUlocationID", "PULocationID"],
    "pub_dropoff_zone":      ["DOlocationID", "DOLocationID"],
    "rate_code_id":          ["Rate_Code", "RatecodeID"],
    "store_and_fwd_raw":     ["store_and_forward", "store_and_fwd_flag"],
    "payment_type_raw":      ["Payment_Type", "payment_type"],
    "fare_amount":           ["Fare_Amt", "fare_amount", "base_passenger_fare"],
    "extra":                 ["surcharge", "extra"],
    "mta_tax":               ["mta_tax"],
    "tip_amount":            ["Tip_Amt", "tip_amount", "tips"],
    "tolls_amount":          ["Tolls_Amt", "tolls_amount", "tolls"],
    "improvement_surcharge": ["improvement_surcharge"],
    "congestion_surcharge":  ["congestion_surcharge"],
    "airport_fee":           ["airport_fee"],
    "cbd_convenience_fee":   ["cbd_convenience_fee"],
    "total_amount":          ["Total_Amt", "total_amount"],
}
STRING_COLS = {"vendor_id", "payment_type_raw", "store_and_fwd_raw"}

# Dates réglementaires d'introduction : avant => non applicable (NULL légitime), après => applicable
INTRO_DATE = {
    "improvement_surcharge": date(2015, 7, 1),    # supplément d'amélioration (2015)
    "congestion_surcharge":  date(2019, 2, 1),    # congestion pricing (2019)
    "airport_fee":           date(2022, 7, 1),    # frais aéroport (apparition dans les fichiers)
    "cbd_convenience_fee":   date(2025, 1, 5),    # péage urbain CBD Manhattan (05/01/2025)
}

FINAL_COLS = ["vtype", "vendor_id", "pickup_datetime", "dropoff_datetime", "passenger_count",
              "trip_distance", "pickup_location_id", "dropoff_location_id", "rate_code_id",
              "store_and_fwd_flag", "payment_type", "fare_amount", "extra", "mta_tax",
              "tip_amount", "tolls_amount", "improvement_surcharge", "congestion_surcharge",
              "airport_fee", "cbd_convenience_fee", "total_amount",
              "improvement_surcharge_applicable", "congestion_surcharge_applicable",
              "airport_fee_applicable", "cbd_convenience_fee_applicable"]


def bronze_taxi_path(vtype, year, month):
    """datalake/bronze/taxi/vtype=yellow/year=2009/month=01/data.parquet"""
    return BRONZE / "taxi" / f"vtype={vtype}" / f"year={year}" / f"month={month:02d}" / "data.parquet"


def bronze_weather_path(year):
    """datalake/bronze/weather/year=2009/data.json"""
    return BRONZE / "weather" / f"year={year}" / "data.json"


def month_iter(y0, m0, y1, m1):
    y, m = y0, m0
    while (y, m) <= (y1, m1):
        yield y, m
        m += 1
        if m == 13:
            y, m = y + 1, 1


def last_published_month(today=None):
    """Dernier mois réellement publié, délai de publication déduit."""
    today = today or date.today()
    total = today.year * 12 + today.month - 1 - PUBLICATION_LAG_MONTHS - 1
    return divmod(total, 12)


def get_spark(app_name="datalake-taxi-nyc"):
    """Session Spark local dimensionnée pour ce poste (RAM limitée, pas d'UDF Python)."""
    from pyspark.sql import SparkSession
    spark_tmp = Path(tempfile.gettempdir()) / "opencode" / "spark_tmp"
    spark_tmp.mkdir(parents=True, exist_ok=True)
    spark = (SparkSession.builder
             .master("local[6]")
             .appName(app_name)
             .config("spark.driver.memory", "2560m")
             .config("spark.local.dir", str(spark_tmp))
             .config("spark.sql.shuffle.partitions", "24")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def bronze_partitions():
    """Partitions taxi présentes en bronze : [(vtype, year, month, chemin)]."""
    out = []
    for bp in sorted(BRONZE.glob("taxi/vtype=*/year=*/month=*/")):
        out.append((bp.parent.parent.name.split("=")[1],
                    int(bp.parent.name.split("=")[1]),
                    int(bp.name.split("=")[1]), bp))
    return out
