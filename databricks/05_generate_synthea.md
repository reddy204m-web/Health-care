# Getting the data (Synthea)

Option A, run Synthea locally (needs Java 17+):
```
git clone https://github.com/synthetichealth/synthea && cd synthea
./run_synthea -p 5000 Massachusetts --exporter.csv.export=true --exporter.fhir.export=false
# CSVs appear in output/csv/
```
Option B, download the sample CSV datasets from the Synthea site (synthea.mitre.org/downloads).

Upload to the lake:
* Azure: container `lake`, folder `source/` -> patients.csv, encounters.csv, conditions.csv,
  providers.csv, payers.csv, procedures.csv
* Databricks Free Edition: upload to a Unity Catalog volume, then copy each file to
  `<lake_root>/landing/<table>/ingest_date=YYYY-MM-DD/<table>.csv`
  (ADF does this step for you on Azure)
