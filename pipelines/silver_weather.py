# -*- coding: utf-8 -*-
"""Silver météo : JSON brut Open-Meteo -> table horaire parquet (petite => driver-side)."""
import json
import sys
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipelines.config import BRONZE, SILVER, get_spark

SCHEMA = T.StructType([
    T.StructField("ts", T.TimestampType()),
    T.StructField("temperature_c", T.DoubleType()),
    T.StructField("humidite_pct", T.DoubleType()),
    T.StructField("precipitation_mm", T.DoubleType()),
    T.StructField("pluie_mm", T.DoubleType()),
    T.StructField("neige_mm", T.DoubleType()),
    T.StructField("vent_kmh", T.DoubleType()),
    T.StructField("code_meteo", T.IntegerType()),
])


def _fnum(v):
    return None if v is None else float(v)


def main(argv=None):
    rows = []
    for jf in sorted(BRONZE.glob("weather/year=*/data.json")):
        hourly = json.loads(jf.read_text(encoding="utf-8"))["hourly"]
        for i, t in enumerate(hourly["time"]):
            code = hourly["weather_code"][i]
            rows.append((datetime.fromisoformat(t),
                         _fnum(hourly["temperature_2m"][i]),
                         _fnum(hourly["relative_humidity_2m"][i]),
                         _fnum(hourly["precipitation"][i]),
                         _fnum(hourly["rain"][i]),
                         _fnum(hourly["snowfall"][i]),
                         _fnum(hourly["wind_speed_10m"][i]),
                         None if code is None else int(code)))

    if not rows:
        print("aucun JSON météo en bronze")
        return 0

    spark = get_spark("silver-weather")
    weather = spark.createDataFrame(rows, SCHEMA).withColumn("year", F.year("ts"))
    weather.write.mode("overwrite").partitionBy("year").parquet(str(SILVER / "weather_hourly"))
    print(weather.count(), "heures de météo ingérées")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
