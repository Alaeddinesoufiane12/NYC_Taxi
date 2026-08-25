# -*- coding: utf-8 -*-
"""Contrôles qualité du silver : volumes, résolution des zones, jointure météo."""
import sys

from pipelines.config import SILVER, get_spark


def main(argv=None):
    spark = get_spark("verify-silver")
    trips = spark.read.parquet(str(SILVER / "trips"))
    trips.createOrReplaceTempView("trips")
    spark.read.parquet(str(SILVER / "ref" / "zones.parquet")).createOrReplaceTempView("zones")

    print("== Courses par type et par année ==")
    spark.sql("""
SELECT vtype, year, count(*) AS courses,
       count(distinct month) AS mois,
       round(count(pickup_location_id) / count(*) * 100, 1) AS pct_pickup_zone_resolue
FROM trips GROUP BY 1, 2 ORDER BY 1, 2""").show(20, False)

    print("== Top 10 zones de prise en charge ==")
    spark.sql("""
SELECT z.borough, z.zone_name, count(*) AS courses
FROM trips t JOIN zones z ON t.pickup_location_id = z.location_id
GROUP BY 1, 2 ORDER BY courses DESC LIMIT 10""").show(10, False)

    print("== Suppléments : moyenne par année et type (NULL = n'existait pas) ==")
    spark.sql("""
SELECT vtype, year,
       round(avg(extra), 3)                AS surcharge_soir_nuit,
       round(avg(mta_tax), 3)              AS mta_tax,
       round(avg(improvement_surcharge), 3) AS amelioration,
       round(avg(congestion_surcharge), 3)  AS congestion
FROM trips GROUP BY 1, 2 ORDER BY 1, 2""").show(30, False)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
