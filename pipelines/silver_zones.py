# -*- coding: utf-8 -*-
"""Silver référentiels : zones TLC (shapefile EPSG:2263 -> WGS84) + grille 0.001 deg.

Produit :
    datalake/silver/ref/zones.parquet        (location_id, borough, zone_name, centroïde)
    datalake/silver/ref/grid_zones.parquet   (maille lon/lat -> location_id)
"""
import zipfile

import numpy as np
import pandas as pd
import requests
import shapefile
from shapely import STRtree, contains_xy
from shapely.geometry import Point, Polygon
from shapely.ops import unary_union
from pyproj import Transformer

from pipelines.config import REF_DIR, SILVER, TLC_ZONES_URL


def build():
    REF_DIR.mkdir(parents=True, exist_ok=True)
    shp_root = REF_DIR / "taxi_zones"
    shp_file = next(shp_root.rglob("taxi_zones.shp"), None) if shp_root.exists() else None
    if shp_file is None:
        zip_dst = REF_DIR / "taxi_zones.zip"
        if not zip_dst.exists():
            zip_dst.write_bytes(requests.get(TLC_ZONES_URL, timeout=120).content)
        with zipfile.ZipFile(zip_dst) as z:
            z.extractall(shp_root)
        shp_file = next(shp_root.rglob("taxi_zones.shp"))

    sf = shapefile.Reader(str(shp_file))
    field_names = [f[0] for f in sf.fields[1:]]           # saute DeletionFlag
    to_wgs84 = Transformer.from_crs("EPSG:2263", "EPSG:4326", always_xy=True)

    rings, ring_locid, by_locid = [], [], {}
    for i, shp in enumerate(sf.shapes()):
        rec = dict(zip(field_names, sf.record(i)))
        lid = int(rec["LocationID"])
        parts = list(shp.parts) + [len(shp.points)]
        polys = []
        for a, b in zip(parts[:-1], parts[1:]):
            ring_pts = shp.points[a:b]
            if len(ring_pts) < 4:
                continue
            poly = Polygon([to_wgs84.transform(x, y) for x, y in ring_pts])
            if not poly.is_valid:
                poly = poly.buffer(0)
            rings.append(poly)
            ring_locid.append(lid)
            polys.append(poly)
        if polys:
            centroid = unary_union(polys).centroid
            by_locid[lid] = (rec["borough"], rec["zone"], centroid.x, centroid.y)

    zones = pd.DataFrame(
        [(lid, b, z, cx, cy) for lid, (b, z, cx, cy) in sorted(by_locid.items())],
        columns=["location_id", "borough", "zone_name", "center_lon", "center_lat"])

    tree = STRtree(rings)
    STEP = 1000                                           # 0.001 deg ~ 110 m
    bounds = np.array([r.bounds for r in rings])
    lon_min, lat_min = bounds[:, [0, 1]].min(axis=0) - 0.002
    lon_max, lat_max = bounds[:, [2, 3]].max(axis=0) + 0.002
    gx, gy = np.meshgrid(np.arange(round(lon_min * STEP), round(lon_max * STEP) + 1),
                         np.arange(round(lat_min * STEP), round(lat_max * STEP) + 1))
    lon_k, lat_k = gx.ravel(), gy.ravel()
    clon, clat = lon_k / STEP, lat_k / STEP

    pairs = tree.query([Point(x, y) for x, y in zip(clon, clat)])
    owner = np.full(len(lon_k), -1, dtype=np.int64)
    for pi, ri in zip(*pairs):
        if owner[pi] == -1 and contains_xy(rings[ri], clon[pi], clat[pi]):
            owner[pi] = ring_locid[ri]
    mask = owner >= 0

    grid = pd.DataFrame({"lon_key": lon_k[mask], "lat_key": lat_k[mask],
                         "location_id": owner[mask].astype(int)})
    grid = grid.merge(zones[["location_id", "borough"]], on="location_id", how="left")

    (SILVER / "ref").mkdir(parents=True, exist_ok=True)
    zones.to_parquet(SILVER / "ref" / "zones.parquet", index=False)
    grid.to_parquet(SILVER / "ref" / "grid_zones.parquet", index=False)

    print(f"{len(zones)} zones | {len(grid)} mailles assignées "
          f"({len(grid) / len(lon_k):.1%} de la bbox NYC)")


if __name__ == "__main__":
    build()
