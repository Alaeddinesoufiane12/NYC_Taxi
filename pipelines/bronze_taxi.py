# -*- coding: utf-8 -*-
"""Bronze taxi : ingestion idempotente des fichiers TLC vers datalake/bronze/taxi.

Usage :
    py -m pipelines.bronze_taxi                # ingère le miroir local (+ rien d'autre)
    py -m pipelines.bronze_taxi --full-history # télécharge TOUT l'historique publié
"""
import argparse
import re
import shutil
import sys

import requests

from pipelines.config import (DATA_DIR, DATALAKE, VEHICLES, TLC_TRIP_DATA,
                              bronze_taxi_path, last_published_month, month_iter)


def ingest_month(vtype, year, month):
    """Ingère un mois d'un type. Retourne l'action effectuée (idempotent)."""
    dst = bronze_taxi_path(vtype, year, month)
    if dst.exists():
        return "déjà ingéré"
    prefix = VEHICLES[vtype][1]
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.parent / (dst.name + ".part")

    local = next((p for p in DATA_DIR.rglob(f"{prefix}_{year}-{month:02d}.parquet")
                  if not str(p).startswith(str(DATALAKE))), None)
    if local is not None:
        shutil.copyfile(local, tmp)               # miroir local : pas de re-téléchargement
        action = f"copié ({local.relative_to(DATA_DIR)})"
    else:
        url = f"{TLC_TRIP_DATA}/{prefix}_{year}-{month:02d}.parquet"
        with requests.get(url, stream=True, timeout=900) as r:
            r.raise_for_status()
            with open(tmp, "wb") as fh:
                shutil.copyfileobj(r.raw, fh, length=8 * 1024 * 1024)
        action = "téléchargé"
    tmp.replace(dst)                              # renommage atomique : jamais de fichier à moitié écrit
    return action


def plan_ingestion(full_history=False):
    """Liste (vtype, année, mois) à ingérer : tout l'historique publié, ou le miroir local."""
    ey, em = last_published_month()
    plan = []
    if full_history:
        for vt, (start, _) in VEHICLES.items():
            sy, sm = map(int, start.split("-"))
            plan += [(vt, y, m) for y, m in month_iter(sy, sm, ey, em)]
    else:
        pat = re.compile(r"(\d{4})-(\d{2})\.parquet$")
        for vt in VEHICLES:
            prefix = VEHICLES[vt][1]
            for p in sorted(DATA_DIR.rglob(f"{prefix}_*.parquet")):
                m = pat.search(p.name)
                if m:
                    plan.append((vt, int(m.group(1)), int(m.group(2))))
    return sorted(set(plan))


def main(argv=None):
    parser = argparse.ArgumentParser(description="Ingestion bronze taxi")
    parser.add_argument("--full-history", action="store_true",
                        help="télécharge tout l'historique TLC publié (centaines de Go)")
    args = parser.parse_args(argv)

    plan = plan_ingestion(args.full_history)
    print(f"{len(plan)} fichier(s) à ingérer\n")

    stats = {}
    for vt, y, m in plan:
        action = ingest_month(vt, y, m)
        stats.setdefault(vt, []).append((y, m))
        print(f"  {action:<28} | {vt:<6} {y}-{m:02d}")

    print()
    for vt, lst in sorted(stats.items()):
        print(f"{vt}: {len(lst)} mois au bronze "
              f"({lst[0][0]}-{lst[0][1]:02d} -> {lst[-1][0]}-{lst[-1][1]:02d})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
