# -*- coding: utf-8 -*-
"""Gold insights : agrégats Spark -> rapport HTML autonome (datalake/gold/insights.html).

Toutes les figures sont rendues en matplotlib (backend Agg) puis embarquées en base64 :
le HTML ne dépend d'aucun serveur ni CDN, il s'ouvre tel quel dans un navigateur.
"""
import base64
import io
import sys
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib.patches import PathPatch, Wedge
from matplotlib.path import Path as MplPath
from pyspark.sql import functions as F

from pipelines.config import GOLD_DIR, SILVER, SITE_DIR, get_spark

ACCENT = "#fb8500"
DARK = "#023047"


def fig_b64(fig):
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", dpi=105, facecolor="white")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode("ascii")


# ----------------------------------------------------------------- figures
def fig_supplements(df_sup):
    vtypes = sorted(df_sup["vtype"].unique())
    fig, axes = plt.subplots(len(vtypes), 1, figsize=(11, 3.1 * len(vtypes)), squeeze=False)
    for ax, vt in zip(axes[:, 0], vtypes):
        d = df_sup[df_sup["vtype"] == vt].set_index("mois").drop(columns="vtype", errors="ignore")
        trace = False
        for col in d.columns:
            s = d[col].dropna()
            if len(s):
                ax.plot(s.index, s.values, marker="o", ms=3.5, lw=1.4, label=col)
                trace = True
        if not trace:
            ax.text(0.5, 0.5, "aucun montant publié pour ce type (FHV : pas de tarification)",
                    ha="center", va="center", transform=ax.transAxes, fontsize=11, color="gray")
        ax.set_title(f"Suppléments moyens par course — {vt}", fontsize=11)
        ax.set_ylabel("$ / course")
        ax.grid(alpha=.3)
        ax.legend(fontsize=8)
        ax.tick_params(axis="x", rotation=60, labelsize=8)
    fig.tight_layout()
    return fig


def fig_chord(mat, title):
    labels = list(mat.index)
    n = len(labels)
    flows_v = mat.values.astype(float)
    node_size = flows_v.sum(axis=1) + flows_v.sum(axis=0)
    colors = plt.cm.tab20(np.linspace(0, 1, max(n, 2)))

    fracs = node_size / node_size.sum()
    sect, a = [], 90.0
    for f in fracs:
        span = 360.0 * f
        sect.append((a - span, a))
        a -= span

    fig, ax = plt.subplots(figsize=(8.6, 8.6))
    ax.set_xlim(-1.5, 1.5); ax.set_ylim(-1.5, 1.5); ax.set_aspect("equal"); ax.axis("off")

    def pt(angle_deg, r=1.0):
        t = np.deg2rad(angle_deg)
        return r * np.cos(t), r * np.sin(t)

    for k, ((a0, a1), lab) in enumerate(zip(sect, labels)):
        ax.add_patch(Wedge((0, 0), 1.02, a0, a1, width=0.04,
                           facecolor=colors[k], edgecolor="none"))
        mid_deg = (a0 + a1) / 2
        mid = np.deg2rad(mid_deg)
        etiquette = f"{lab} ({node_size[k]:,.0f})"
        if a1 - a0 < 25:
            m = mid_deg % 360
            rot, ha = (mid_deg + 180, "right") if 90 < m < 270 else (mid_deg, "left")
            ax.text(1.06 * np.cos(mid), 1.06 * np.sin(mid), etiquette,
                    ha=ha, va="center", rotation=rot, rotation_mode="anchor",
                    fontsize=9, color=colors[k])
        else:
            rot = mid_deg if 0 <= (mid_deg % 360) <= 180 else mid_deg + 180
            ax.text(1.12 * np.cos(mid), 1.12 * np.sin(mid), etiquette,
                    ha="center", va="center", rotation=rot, rotation_mode="anchor",
                    fontsize=9, color=colors[k])

    out_cur = {k: sect[k][0] for k in range(n)}
    in_cur = {k: sect[k][0] for k in range(n)}
    K = 0.15
    for i in range(n):
        for j in range(n):
            v = flows_v[i, j]
            if v <= 0:
                continue
            i0, i1 = out_cur[i], out_cur[i] + 360.0 * v / node_size[i]; out_cur[i] = i1
            j0, j1 = in_cur[j], in_cur[j] + 360.0 * v / node_size[j]; in_cur[j] = j1
            if i == j:
                continue
            e0a, e0b, e1a, e1b = pt(i0), pt(i1), pt(j0), pt(j1)
            verts = [e0a,
                     (K * e0a[0], K * e0a[1]), (K * e1a[0], K * e1a[1]), e1a,
                     e1b,
                     (K * e1b[0], K * e1b[1]), (K * e0b[0], K * e0b[1]), e0b,
                     e0a]
            codes = [MplPath.MOVETO, MplPath.CURVE4, MplPath.CURVE4, MplPath.CURVE4,
                     MplPath.LINETO, MplPath.CURVE4, MplPath.CURVE4, MplPath.CURVE4,
                     MplPath.CLOSEPOLY]
            ax.add_patch(PathPatch(MplPath(verts, codes),
                                   facecolor=colors[i], alpha=0.30, edgecolor="none"))
    ax.set_title(title, pad=30, fontsize=12)
    fig.tight_layout()
    return fig


def fig_ridgeline(hist, zone_name, period_fmt):
    BIN_W = 0.5
    xs = np.arange(hist["bin"].min(), hist["bin"].max() + 1) * BIN_W + BIN_W / 2
    curves = {}
    for periode, grp in hist.groupby("periode"):
        dens = np.zeros(len(xs))
        for _, r in grp.iterrows():
            idx = int(round((r["bin"] * BIN_W + BIN_W / 2 - xs[0]) / BIN_W))
            if 0 <= idx < len(dens):
                dens[idx] += r["n"]
        total = dens.sum()
        dens = dens / (total * BIN_W) if total else dens
        kern = np.array([1, 4, 6, 4, 1]) / 16.0
        curves[periode] = np.convolve(np.convolve(dens, kern, "same"), kern, "same")

    order = sorted(curves)
    fig, ax = plt.subplots(figsize=(9.5, 0.95 * len(order) + 1.4))
    palette = plt.cm.viridis(np.linspace(0.05, 0.9, len(order)))
    ymax = max(c.max() for c in curves.values())
    for k, periode in enumerate(order):
        base = 1.0 * (len(order) - 1 - k)
        y = curves[periode] / ymax
        ax.fill_between(xs, base, base + y, color="white", zorder=k + 1)
        ax.plot(xs, base + y, color=palette[k], lw=1.4, zorder=k + 1)
        ax.text(xs[0] - 0.3, base + 0.08, periode, ha="right", fontsize=9, color=palette[k])
    ax.set_xlim(xs[0], xs[-1]); ax.set_ylim(-0.2, len(order) + 0.6)
    ax.set_yticks([])
    ax.set_xlabel("$ / km")
    ax.grid(alpha=.25, axis="x")
    ax.set_title(f"Distribution du prix au km par {'année' if period_fmt == 'yyyy' else 'mois'} "
                 f"— zone de départ : {zone_name}", fontsize=12)
    fig.tight_layout()
    return fig


def fig_heatmap_annee(dfh, annee):
    JOURS = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]
    vtypes = sorted(dfh["vtype"].unique())
    fig, axes = plt.subplots(1, len(vtypes), figsize=(4.6 * len(vtypes), 4.4), squeeze=False)
    for ax, vt in zip(axes[0], vtypes):
        piv = (dfh[dfh["vtype"] == vt]
               .pivot(index="jour", columns="heure", values="courses")
               .reindex(index=range(7)).fillna(0))
        sns.heatmap(piv, ax=ax, cmap="magma", cbar_kws={"label": "courses"})
        ax.set_title(f"{vt} — {annee}", fontsize=11)
        ax.set_yticklabels(JOURS, rotation=0, fontsize=8)
        ax.set_xlabel("Heure")
        ax.set_ylabel("")
        ax.tick_params(axis="x", labelsize=8)
    fig.tight_layout()
    return fig


def fig_meteo(meteo_prix, moyenne_globale):
    fig, ax = plt.subplots(figsize=(6.6, 3.9))
    colors_ = {"sec": "#8ecae6", "pluie": DARK}
    bars = ax.bar(meteo_prix["conditions"], meteo_prix["prix_km_moyen"],
                  color=[colors_.get(c, "gray") for c in meteo_prix["conditions"]])
    ax.axhline(moyenne_globale, color=ACCENT, ls="--", lw=1.5,
               label=f"moyenne globale ({moyenne_globale:.2f} $/km)")
    for b, ecart in zip(bars, meteo_prix["ecart_pct"]):
        ax.annotate(f"{ecart:+.1f} %", (b.get_x() + b.get_width() / 2, b.get_height()),
                    ha="center", va="bottom", fontsize=11, fontweight="bold")
    ax.set_ylabel("Prix moyen ($/km)")
    ax.set_title("Écart de prix au km selon la météo au moment de la course", fontsize=12)
    ax.legend()
    fig.tight_layout()
    return fig


# ----------------------------------------------------------------- html
CSS = """
body{font-family:'Segoe UI',system-ui,-apple-system,sans-serif;margin:0;background:#f4f6f8;color:#1c2833}
header{background:linear-gradient(135deg,#023047,#12678e);color:#fff;padding:34px 8% 28px}
header h1{margin:0 0 6px;font-size:26px} header p{margin:0;opacity:.85;font-size:14px}
main{max-width:1180px;margin:0 auto;padding:26px 18px 60px}
.kpis{display:flex;flex-wrap:wrap;gap:14px;margin:-46px 0 30px}
.kpi{flex:1 1 160px;background:#fff;border-radius:12px;padding:16px 18px;box-shadow:0 3px 14px rgba(2,48,71,.12);border-top:4px solid #fb8500}
.kpi .v{font-size:24px;font-weight:700;color:#023047} .kpi .l{font-size:12px;color:#5d6d7e;margin-top:2px}
section{background:#fff;border-radius:12px;padding:24px 28px;margin-bottom:26px;box-shadow:0 2px 10px rgba(2,48,71,.08)}
section h2{margin:0 0 4px;font-size:19px;color:#023047;border-left:5px solid #fb8500;padding-left:12px}
section p.desc{color:#5d6d7e;font-size:13.5px;margin:8px 0 16px}
img{max-width:100%;height:auto;display:block;margin:6px auto}
table{border-collapse:collapse;width:100%;font-size:13.5px;margin-top:10px}
th{background:#023047;color:#fff;padding:8px 10px;text-align:left;font-weight:600}
td{padding:7px 10px;border-bottom:1px solid #e8edf1}
tr:nth-child(even) td{background:#f7fafc}
.num{text-align:right;font-variant-numeric:tabular-nums}
footer{text-align:center;color:#7f8c8d;font-size:12px;padding:18px}
.badge{display:inline-block;background:#eef4f8;color:#12678e;border-radius:20px;padding:3px 12px;font-size:12px;margin:4px 6px 0 0}
"""

TPL = """<!DOCTYPE html>
<html lang="fr"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Insights — Datalake NYC Taxi × Météo</title>
<style>{css}</style></head>
<body>
<header>
  <h1>Datalake NYC Taxi × Météo — Insights</h1>
  <p>Rapport généré le {generated} depuis la couche silver ({total_courses} courses, {perimetre})</p>
</header>
<main>
  <div class="kpis">{kpis}</div>
  {sections}
</main>
<footer>Pipeline medallion : make all &nbsp;•&nbsp; bronze → silver → gold &nbsp;•&nbsp; source : NYC TLC &amp; Open-Meteo &nbsp;•&nbsp; <a href="https://github.com/Alaeddinesoufiane12/NYC_Taxi" style="color:#7f8c8d">code source</a></footer>
</body></html>"""


def kpi(value, label):
    return f'<div class="kpi"><div class="v">{value}</div><div class="l">{label}</div></div>'


def section(num, titre, description, corps):
    return (f'<section><h2>{num}. {titre}</h2>'
            f'<p class="desc">{description}</p>{corps}</section>')


def df_html(df, num_cols=()):
    cols = list(df.columns)
    th = "".join(f'<th class="{"num" if c in num_cols else ""}">{c}</th>' for c in cols)
    rows = []
    for _, r in df.iterrows():
        tds = "".join(f'<td class="{"num" if c in num_cols else ""}">{r[c]}</td>' for c in cols)
        rows.append(f"<tr>{tds}</tr>")
    return f'<table><thead><tr>{th}</tr></thead><tbody>{"".join(rows)}</tbody></table>'


# ----------------------------------------------------------------- main
def main(argv=None):
    spark = get_spark("gold-insights")
    trips = spark.read.parquet(str(SILVER / "trips"))
    trips.createOrReplaceTempView("trips")
    spark.read.parquet(str(SILVER / "ref" / "zones.parquet")).createOrReplaceTempView("zones")
    spark.read.parquet(str(SILVER / "weather_hourly")).createOrReplaceTempView("weather_hourly")

    print("[1/7] volumes et KPI...")
    volumes = spark.sql("""
SELECT vtype, year, count(*) AS courses,
       round(count(pickup_location_id) / count(*) * 100, 1) AS pct_zone_resolue
FROM trips GROUP BY 1, 2 ORDER BY 1, 2""").toPandas()
    total = int(volumes["courses"].sum())
    perimetre = ", ".join(f"{r.vtype} {r.year}" for r in volumes.itertuples())

    print("[2/7] suppléments...")
    sup = spark.sql("""
SELECT date_format(pickup_datetime, 'yyyy-MM') AS mois, vtype,
       avg(extra) AS surcharge_soir_nuit, avg(mta_tax) AS mta_tax,
       avg(improvement_surcharge) AS amelioration, avg(congestion_surcharge) AS congestion,
       avg(airport_fee) AS aeroport, avg(cbd_convenience_fee) AS cbd
FROM trips GROUP BY 1, 2 ORDER BY 1, 2""").toPandas()

    print("[3/7] flux boroughs...")
    flows = spark.sql("""
SELECT zb.borough AS depart, zd.borough AS arrivee, count(*) AS courses
FROM trips t
JOIN zones zb ON t.pickup_location_id = zb.location_id
JOIN zones zd ON t.dropoff_location_id = zd.location_id
WHERE t.pickup_location_id <> t.dropoff_location_id
GROUP BY 1, 2""").toPandas()
    labels = sorted(set(flows["depart"]) | set(flows["arrivee"]))
    mat = (flows.pivot(index="depart", columns="arrivee", values="courses")
           .reindex(index=labels, columns=labels).fillna(0.0))

    print("[4/7] ridgeline prix/km...")
    top_zone = spark.sql("""
SELECT z.zone_name, count(*) AS c
FROM trips t JOIN zones z ON t.pickup_location_id = z.location_id
WHERE t.trip_distance BETWEEN 0.5 AND 50 AND t.fare_amount > 2.5
GROUP BY 1 ORDER BY c DESC LIMIT 1""").first()["zone_name"]
    loc_id = (spark.read.parquet(str(SILVER / "ref" / "zones.parquet"))
              .where(F.col("zone_name") == top_zone).first()["location_id"])
    period_fmt = ("yyyy-MM" if volumes["year"].nunique() == 1 else "yyyy")
    hist = spark.sql(f"""
WITH base AS (
    SELECT date_format(pickup_datetime, '{period_fmt}') AS periode,
           fare_amount / trip_distance AS prix_km
    FROM trips
    WHERE pickup_location_id = {int(loc_id)}
      AND trip_distance BETWEEN 0.5 AND 50 AND fare_amount > 2.5
      AND fare_amount / trip_distance < 20
)
SELECT periode, cast(floor(prix_km / 0.5) AS int) AS bin, count(*) AS n
FROM base GROUP BY 1, 2""").toPandas()

    print("[5/7] heatmaps...")
    annees = sorted(volumes["year"].unique())
    imgs_heat = []
    for annee in annees:
        dfh = spark.sql(f"""
SELECT vtype, pmod(dayofweek(pickup_datetime) + 5, 7) AS jour,
       hour(pickup_datetime) AS heure, count(*) AS courses
FROM trips WHERE year = {int(annee)} GROUP BY 1, 2, 3""").toPandas()
        imgs_heat.append(fig_b64(fig_heatmap_annee(dfh, int(annee))))

    print("[6/7] météo...")
    meteo = spark.sql("""
SELECT /*+ BROADCAST(w) */
       CASE WHEN w.precipitation_mm > 0.2 THEN 'pluie' ELSE 'sec' END AS conditions,
       count(*) AS courses, avg(t.fare_amount / t.trip_distance) AS prix_km_moyen
FROM trips t
JOIN weather_hourly w ON date_trunc('hour', t.pickup_datetime) = w.ts
WHERE t.trip_distance BETWEEN 0.5 AND 50 AND t.fare_amount > 2.5
GROUP BY 1""").toPandas()
    overall = spark.sql("""
SELECT avg(fare_amount / trip_distance) AS m, count(*) AS n
FROM trips WHERE trip_distance BETWEEN 0.5 AND 50 AND fare_amount > 2.5""").first()
    moyenne_globale = overall["m"]
    meteo["ecart_pct"] = 100 * (meteo["prix_km_moyen"] / moyenne_globale - 1)

    print("[7/7] rendu HTML...")
    # figures
    imgs_sup = fig_b64(fig_supplements(sup))
    img_chord = fig_b64(fig_chord(mat, "Flux de courses entre boroughs — pickup vers dropoff"))
    img_ridge = fig_b64(fig_ridgeline(hist, top_zone, period_fmt))
    img_meteo = fig_b64(fig_meteo(meteo, moyenne_globale))

    # KPI
    pct_global = round(float((volumes["courses"] * volumes["pct_zone_resolue"]).sum()
                             / volumes["courses"].sum()), 1)
    delta_pluie = float(meteo.loc[meteo["conditions"] == "pluie", "ecart_pct"].mean()) \
        if (meteo["conditions"] == "pluie").any() else 0.0
    annees_txt = f"{volumes['year'].min()}–{volumes['year'].max()}" \
        if volumes["year"].nunique() > 1 else str(volumes["year"].iloc[0])
    kpis = "".join([
        kpi(f"{total:,}".replace(",", " "), "courses analysées"),
        kpi(str(volumes["vtype"].nunique()), "types de véhicules"),
        kpi(annees_txt, "période couverte"),
        kpi(f"{pct_global} %", "courses rattachées à une zone"),
        kpi(f"{moyenne_globale:.2f} $/km", "prix moyen au km"),
        kpi(f"{delta_pluie:+.1f} %", "prix au km sous la pluie"),
    ])

    vol_tbl = volumes.copy()
    vol_tbl["courses"] = vol_tbl["courses"].map(lambda x: f"{x:,}".replace(",", " "))
    top_zones = spark.sql("""
SELECT z.borough, z.zone_name, count(*) AS courses
FROM trips t JOIN zones z ON t.pickup_location_id = z.location_id
GROUP BY 1, 2 ORDER BY courses DESC LIMIT 10""").toPandas()
    top_zones["courses"] = top_zones["courses"].map(lambda x: f"{x:,}".replace(",", " "))

    sections = "".join([
        section(1, "Évolution des suppléments tarifaires",
                "Montant moyen par course et par type. Une courbe qui démarre en cours de période "
                "signale une taxe apparue progressivement : le silver distingue le NULL "
                "(« n'existait pas encore ») du zéro (« applicable, non facturé »).",
                f'<img src="data:image/png;base64,{imgs_sup}" alt="suppléments">'),
        section(2, "Flux pickup → dropoff entre boroughs",
                "Courses inter-boroughs ; la largeur de chaque ruban est proportionnelle au nombre "
                "de courses. Les flux internes à un borough consomment son secteur sans être dessinés.",
                f'<img src="data:image/png;base64,{img_chord}" alt="chord diagram">'),
        section(3, "Ridgeline du prix au km — zone la plus fréquente",
                f"Zone : <b>{top_zone}</b>. Filtres : distance 0,5–50 km, tarif &gt; 2,50 $ "
                "(les courses FHV, sans montants, sortent du périmètre).",
                f'<img src="data:image/png;base64,{img_ridge}" alt="ridgeline">'),
        section(4, "Fréquence des courses — jour × heure",
                "Une heatmap par année et par type de véhicule : pics de semaine 17–19 h, "
                "week-ends décalés après minuit.",
                "".join(f'<img src="data:image/png;base64,{b}" alt="heatmap {a}">'
                        for a, b in zip(annees, imgs_heat))),
        section(5, "Écart de prix selon la météo",
                "Prix moyen au km des courses prises en charge sous la pluie (&gt; 0,2 mm/h) "
                "contre au sec, comparé à la moyenne générale.",
                f'<img src="data:image/png;base64,{img_meteo}" alt="météo">'
                + df_html(meteo[["conditions", "courses", "prix_km_moyen", "ecart_pct"]],
                          num_cols={"courses", "prix_km_moyen", "ecart_pct"})),
        section(6, "Top 10 des zones de prise en charge",
                "Toutes années et tous types confondus.",
                df_html(top_zones, num_cols={"courses"})),
        section(7, "Volumes ingérés par type et par année",
                "Part des courses rattachées à une zone TLC : GPS géocodé via grille avant juillet 2016, "
                "LocationID publié ensuite.",
                df_html(vol_tbl, num_cols={"courses", "pct_zone_resolue"})),
    ])

    html = TPL.format(css=CSS, generated=datetime.now().strftime("%d/%m/%Y %H:%M"),
                      total_courses=f"{total:,}".replace(",", " "), perimetre=perimetre,
                      kpis=kpis, sections=sections)

    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    out = GOLD_DIR / "insights.html"
    out.write_text(html, encoding="utf-8")
    print(f"rapport écrit : {out} ({out.stat().st_size / 1024:.0f} Ko)")

    # copie pour le déploiement statique Vercel (site/index.html, versionné dans git)
    SITE_DIR.mkdir(parents=True, exist_ok=True)
    site = SITE_DIR / "index.html"
    site.write_text(html, encoding="utf-8")
    print(f"site Vercel  : {site}")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
