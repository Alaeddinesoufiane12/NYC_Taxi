# -*- coding: utf-8 -*-
"""Silver trips : bronze -> normalisation -> nettoyage -> réconciliation zones -> parquet.

Idempotent par partition (un répertoire cible non vide => skip ; le supprimer pour forcer).
Aucune UDF Python : tout passe par Catalyst, indispensable en local[6] sur 160M+ lignes.
"""
import sys
from datetime import date, datetime

from pyspark.sql import functions as F

from pipelines.config import (BRONZE, FINAL_COLS, INTRO_DATE, COLMAP, STRING_COLS,
                              SILVER, bronze_partitions, get_spark)


def normalize(df):
    """Projette n'importe quel schéma historique vers le schéma canonique."""
    cols = set(df.columns)
    exprs = []
    for name, candidates in COLMAP.items():
        src = next((c for c in candidates if c in cols), None)
        if src is None:
            exprs.append(F.lit(None).alias(name))          # colonne absente => NULL (jamais 0 !)
        elif name in STRING_COLS:
            exprs.append(F.col(src).cast("string").alias(name))
        else:
            exprs.append(F.col(src).alias(name))
    out = df.select(*exprs)

    def money(c):
        col = F.col(c).cast("double")
        return F.when(col.isNull() | F.isnan(col), None).otherwise(col)

    pt = F.lower(F.trim(F.col("payment_type_raw")))
    sfwd = F.upper(F.trim(F.col("store_and_fwd_raw")))
    rc = money("rate_code_id")

    out = (out
        .withColumn("pickup_datetime", F.col("pickup_datetime").cast("timestamp"))
        .withColumn("dropoff_datetime", F.col("dropoff_datetime").cast("timestamp"))
        .withColumn("passenger_count", F.col("passenger_count").cast("int"))
        .withColumn("trip_distance", money("trip_distance"))
        .withColumn("trip_distance",
                    F.when(F.col("trip_distance").between(0, 200), F.col("trip_distance")))
        .withColumn("rate_code_id", F.when(rc.isNull(), None).otherwise(rc.cast("int")))
        .withColumn("store_and_fwd_flag",
                    F.when(sfwd.isin("1", "Y", "TRUE"), "Y")
                     .when(sfwd.isin("0", "N", "FALSE"), "N"))
        .withColumn("payment_type",
                    F.when(pt.isin("credit", "1"), "carte").when(pt.isin("cash", "2"), "especes")
                     .when(pt.isin("no charge", "3"), "sans_frais")
                     .when(pt.isin("dispute", "4"), "litige")
                     .when(pt.isin("unknown", "5"), "inconnu").when(pt == "6", "annule")
                     .when(pt.isNotNull(), "autre"))
        .withColumn("fare_amount", money("fare_amount"))
        .withColumn("extra", money("extra"))
        .withColumn("mta_tax", money("mta_tax"))
        .withColumn("tip_amount", money("tip_amount"))
        .withColumn("tolls_amount", money("tolls_amount"))
        .withColumn("improvement_surcharge", money("improvement_surcharge"))
        .withColumn("congestion_surcharge", money("congestion_surcharge"))
        .withColumn("airport_fee", money("airport_fee"))
        .withColumn("cbd_convenience_fee", money("cbd_convenience_fee"))
        .withColumn("total_amount", money("total_amount")))

    pickup_day = F.to_date("pickup_datetime")
    for colname, d0 in INTRO_DATE.items():
        out = out.withColumn(f"{colname}_applicable", (pickup_day >= F.lit(d0)))

    # Sentinelle FHV 2016 : dropOff_datetime = 1989-01-01 => dropoff inconnu (NULL),
    # la ligne reste valable pour les analyses de prise en charge.
    out = out.withColumn(
        "dropoff_datetime",
        F.when(F.col("dropoff_datetime") < F.lit(datetime(1990, 1, 1)), None)
         .otherwise(F.col("dropoff_datetime")))
    return out


def resolve_zones(df, grid_zones):
    """GPS brut -> grille -> zone TLC ; sinon LocationID publié ; sinon NULL (outlier)."""
    gz_pu = grid_zones.selectExpr("lon_key as pu_lon_k", "lat_key as pu_lat_k",
                                  "location_id as gps_pu")
    gz_do = grid_zones.selectExpr("lon_key as do_lon_k", "lat_key as do_lat_k",
                                  "location_id as gps_do")
    df = (df
        .withColumn("pu_lon_k", F.round(F.col("pickup_longitude") * F.lit(1000)).cast("int"))
        .withColumn("pu_lat_k", F.round(F.col("pickup_latitude") * F.lit(1000)).cast("int"))
        .withColumn("do_lon_k", F.round(F.col("dropoff_longitude") * F.lit(1000)).cast("int"))
        .withColumn("do_lat_k", F.round(F.col("dropoff_latitude") * F.lit(1000)).cast("int"))
        .join(F.broadcast(gz_pu), ["pu_lon_k", "pu_lat_k"], "left")
        .join(F.broadcast(gz_do), ["do_lon_k", "do_lat_k"], "left")
        .withColumn("pickup_location_id",
                    F.coalesce(F.col("gps_pu"), F.col("pub_pickup_zone").cast("int")))
        .withColumn("dropoff_location_id",
                    F.coalesce(F.col("gps_do"), F.col("pub_dropoff_zone").cast("int"))))
    return df.drop("pu_lon_k", "pu_lat_k", "do_lon_k", "do_lat_k",
                   "gps_pu", "gps_do", "pub_pickup_zone", "pub_dropoff_zone",
                   "pickup_longitude", "pickup_latitude",
                   "dropoff_longitude", "dropoff_latitude")


def main(argv=None):
    spark = get_spark("silver-trips")
    grid_zones = spark.read.parquet(str(SILVER / "ref" / "grid_zones.parquet"))

    parts = bronze_partitions()
    if not parts:
        print("aucune partition bronze trouvée")
        return 1

    ok = (F.col("pickup_datetime").isNotNull() &
          (F.col("dropoff_datetime").isNull() |
           ((F.col("dropoff_datetime") >= F.col("pickup_datetime")) &
            ((F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) <= 24 * 3600))) &
          (F.col("total_amount").isNull() | (F.col("total_amount") >= 0)))

    for vtype, year, month, bp in parts:
        target = SILVER / "trips" / f"vtype={vtype}" / f"year={year}" / f"month={month}"
        if target.exists() and any(target.iterdir()):
            print(f"  déjà traitée | {vtype} {year}-{month:02d}")
            continue

        raw = spark.read.parquet(str(bp))
        res = (resolve_zones(normalize(raw), grid_zones)
            .where(ok)
            .withColumn("vtype", F.lit(vtype))
            .withColumn("year", F.year("pickup_datetime"))
            .withColumn("month", F.month("pickup_datetime"))
            .select(*FINAL_COLS, "year", "month"))

        res.write.mode("append").partitionBy("vtype", "year", "month").parquet(str(SILVER / "trips"))
        print(f"  écrit       | {vtype} {year}-{month:02d}")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
