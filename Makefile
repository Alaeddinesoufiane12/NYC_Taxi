# Datalake NYC Taxi x Meteo - pipeline medallion + insights (notebook + vue HTML)
# Usage : make all   (ou simplement "make")

PY := py

.DEFAULT_GOAL := help
.PHONY: help all bronze silver verify gold insights clean

help:
	@echo "Datalake NYC Taxi x Meteo"
	@echo "Cibles :"
	@echo "  make all      pipeline complet : bronze -> silver -> verify -> gold (HTML) -> notebook"
	@echo "  make bronze   ingestion brute taxi + meteo depuis data/ (idempotent)"
	@echo "  make silver   referentiels zones, nettoyage courses, meteo horaire (idempotent)"
	@echo "  make verify   controles qualite du silver"
	@echo "  make gold     rapport HTML autonome : datalake/gold/insights.html"
	@echo "  make insights execute le notebook d'analyses notebooks/insights_gold.ipynb"
	@echo "  make clean    supprime silver et gold (bronze conserve)"

all: bronze silver verify gold insights

bronze:
	$(PY) -m pipelines.bronze_taxi
	$(PY) -m pipelines.bronze_weather

silver:
	$(PY) -m pipelines.silver_zones
	$(PY) -m pipelines.silver_trips
	$(PY) -m pipelines.silver_weather

verify:
	$(PY) -m pipelines.verify_silver

gold:
	$(PY) -m pipelines.gold_insights

insights:
	$(PY) -m nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=7200 notebooks/insights_gold.ipynb

clean:
	$(PY) -c "import shutil; [shutil.rmtree(p, ignore_errors=True) for p in ('datalake/silver', 'datalake/gold')]; print('silver + gold supprimes')"
