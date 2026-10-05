# Databricks notebook source
# Shared config. Include in other notebooks with:  # MAGIC %run ./00_config
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

dbutils.widgets.text("lake_root", "/Volumes/main/healthcare/lake")  # Free Edition: UC volume path
dbutils.widgets.text("run_date", "")                                  # yyyy-MM-dd, blank = all landed data

# --- Azure Databricks + ADLS Gen2 (uncomment, store the key in a secret scope) ---
# lake_root = abfss://lake@<storageaccount>.dfs.core.windows.net
# spark.conf.set("fs.azure.account.key.<storageaccount>.dfs.core.windows.net",
#                dbutils.secrets.get("kv-scope", "adls-key"))

LAKE = dbutils.widgets.get("lake_root").rstrip("/")
RUN_DATE = dbutils.widgets.get("run_date")
LANDING, BRONZE, SILVER, GOLD = (f"{LAKE}/{x}" for x in ("landing", "bronze", "silver", "gold"))
TABLES = ["patients", "encounters", "conditions", "providers", "payers", "procedures"]

def dedupe(df, key, order_col="_ingest_ts"):
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
