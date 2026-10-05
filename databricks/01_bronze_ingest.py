# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------
# BRONZE: raw copy + lineage metadata, append-only Delta
for t in TABLES:
    folder = f"ingest_date={RUN_DATE}" if RUN_DATE else "ingest_date=*"
    path = f"{LANDING}/{t}/{folder}/*.csv"
    df = (spark.read.option("header", True).option("inferSchema", False)  # keep raw as strings
          .csv(path)
          .select("*", F.col("_metadata.file_path").alias("_source_file"))
          .withColumn("_ingest_ts", F.current_timestamp())
          .withColumn("_run_date", F.lit(RUN_DATE or "all")))
    df.write.format("delta").mode("append").save(f"{BRONZE}/{t}")
    print(f"bronze.{t}: {df.count()} rows")
