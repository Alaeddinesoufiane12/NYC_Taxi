# -*- coding: utf-8 -*-
"""Bronze météo : réponses brutes de l'API Open-Meteo (JSON) vers datalake/bronze/weather.

La persistance est cohérente avec la source : une API => on stocke la réponse JSON telle quelle.
Idempotent par année. Les années traitées sont celles présentes en bronze taxi.
"""
import json
import sys

import requests

from pipelines.config import (BRONZE, HOURLY_VARS, NYC_POINT, OPEN_METEO_ARCHIVE,
                              bronze_partitions, bronze_weather_path)


def fetch_weather_year(year):
    dst = bronze_weather_path(year)
    if dst.exists():
        return "déjà ingéré"
    params = dict(NYC_POINT,
                  start_date=f"{year}-01-01",
                  end_date=f"{year}-12-31",
                  hourly=",".join(HOURLY_VARS))
    r = requests.get(OPEN_METEO_ARCHIVE, params=params, timeout=300)
    r.raise_for_status()
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.parent / (dst.name + ".part")
    tmp.write_text(json.dumps(r.json()), encoding="utf-8")
    tmp.replace(dst)
    return "ingéré"


def main(argv=None):
    years = sorted({y for _, y, _, _ in bronze_partitions()}) or [None]
    if not years[0]:
        print("aucune donnée taxi en bronze : rien à faire pour la météo")
        return 0
    for y in years:
        print(y, "->", fetch_weather_year(y))
    return 0


if __name__ == "__main__":
    sys.exit(main())
