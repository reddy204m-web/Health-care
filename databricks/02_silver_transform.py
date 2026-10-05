# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------
def scd2_merge(updates, target_path, key, track_cols):
    """Slowly changing dimension type 2 using Delta MERGE."""
    updates = updates.withColumn("row_hash", F.sha2(F.concat_ws("||", *track_cols), 256))
    if not DeltaTable.isDeltaTable(spark, target_path):
        (updates.withColumn("effective_from", F.current_timestamp())
                .withColumn("effective_to", F.lit(None).cast("timestamp"))
                .withColumn("is_current", F.lit(True))
                .write.format("delta").save(target_path))
        return
    tgt = DeltaTable.forPath(spark, target_path)
    cur = tgt.toDF().filter("is_current").select(key, F.col("row_hash").alias("t_hash"))
    changed = updates.join(cur, key).filter("row_hash <> t_hash").drop("t_hash")
    staged = (changed.withColumn("merge_key", F.lit(None).cast("string"))
              .unionByName(updates.withColumn("merge_key", F.col(key).cast("string"))))
    ins = {c: f"s.{c}" for c in updates.columns}
    ins.update({"effective_from": "current_timestamp()", "effective_to": "null", "is_current": "true"})
    (tgt.alias("t").merge(staged.alias("s"), f"t.{key} = s.merge_key AND t.is_current = true")
        .whenMatchedUpdate(condition="t.row_hash <> s.row_hash",
                           set={"is_current": "false", "effective_to": "current_timestamp()"})
        .whenNotMatchedInsert(values=ins).execute())

# COMMAND ----------
# PATIENTS: drop direct identifiers, hash name, SCD2 on address
p = dedupe(spark.read.format("delta").load(f"{BRONZE}/patients"), "Id")
p = (p.select(
        F.col("Id").alias("patient_id"),
        F.to_date("BIRTHDATE").alias("birth_date"),
        F.to_date("DEATHDATE").alias("death_date"),
        F.sha2(F.concat_ws(" ", "FIRST", "LAST"), 256).alias("name_hash"),   # SSN/DRIVERS/PASSPORT dropped
        F.upper("GENDER").alias("gender"), "RACE", "ETHNICITY", "MARITAL",
        F.initcap("CITY").alias("city"), F.upper("STATE").alias("state"), "COUNTY", "ZIP",
        F.col("INCOME").cast("double").alias("income"),
        F.col("HEALTHCARE_EXPENSES").cast("double").alias("healthcare_expenses"),
        F.col("HEALTHCARE_COVERAGE").cast("double").alias("healthcare_coverage")))
scd2_merge(p, f"{SILVER}/patients", "patient_id", ["city", "state", "zip"])

# COMMAND ----------
# ENCOUNTERS
e = dedupe(spark.read.format("delta").load(f"{BRONZE}/encounters"), "Id")
e = (e.select(
        F.col("Id").alias("encounter_id"), F.col("PATIENT").alias("patient_id"),
        F.col("PROVIDER").alias("provider_id"), F.col("PAYER").alias("payer_id"),
        F.col("ORGANIZATION").alias("organization_id"),
        F.to_timestamp("START").alias("start_ts"), F.to_timestamp("STOP").alias("stop_ts"),
        F.lower("ENCOUNTERCLASS").alias("encounter_class"),
        F.col("CODE").alias("encounter_code"), F.col("DESCRIPTION").alias("encounter_desc"),
        F.col("BASE_ENCOUNTER_COST").cast("double").alias("base_cost"),
        F.col("TOTAL_CLAIM_COST").cast("double").alias("total_claim_cost"),
        F.col("PAYER_COVERAGE").cast("double").alias("payer_coverage"),
        F.col("REASONCODE").alias("reason_code"), F.col("REASONDESCRIPTION").alias("reason_desc"))
     .filter("encounter_id IS NOT NULL AND patient_id IS NOT NULL AND start_ts IS NOT NULL")
     .withColumn("dq_negative_cost", F.col("total_claim_cost") < 0)
     .withColumn("dq_stop_before_start", F.col("stop_ts") < F.col("start_ts")))
e.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{SILVER}/encounters")

# COMMAND ----------
# CONDITIONS, PROVIDERS, PAYERS, PROCEDURES
c = (spark.read.format("delta").load(f"{BRONZE}/conditions")
     .select(F.col("PATIENT").alias("patient_id"), F.col("ENCOUNTER").alias("encounter_id"),
             F.col("CODE").alias("snomed_code"), F.col("DESCRIPTION").alias("condition_desc"),
             F.to_date("START").alias("start_date"), F.to_date("STOP").alias("stop_date"))
     .dropDuplicates(["patient_id", "encounter_id", "snomed_code"]))
c.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{SILVER}/conditions")

pr = dedupe(spark.read.format("delta").load(f"{BRONZE}/providers"), "Id").select(
        F.col("Id").alias("provider_id"), F.col("ORGANIZATION").alias("organization_id"),
        F.col("NAME").alias("provider_name"), F.col("SPECIALITY").alias("specialty"),
        F.col("CITY").alias("city"), F.col("STATE").alias("state"))
pr.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{SILVER}/providers")

py = dedupe(spark.read.format("delta").load(f"{BRONZE}/payers"), "Id").select(
        F.col("Id").alias("payer_id"), F.col("NAME").alias("payer_name"))
py.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{SILVER}/payers")

pc = (spark.read.format("delta").load(f"{BRONZE}/procedures")
      .select(F.col("PATIENT").alias("patient_id"), F.col("ENCOUNTER").alias("encounter_id"),
              F.col("CODE").alias("procedure_code"), F.col("DESCRIPTION").alias("procedure_desc"),
              F.col("BASE_COST").cast("double").alias("base_cost")))
pc.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{SILVER}/procedures")

# COMMAND ----------
# Data quality summary (feeds the data-quality assistant idea)
dq = spark.read.format("delta").load(f"{SILVER}/encounters").agg(
    F.count("*").alias("rows"),
    F.sum(F.col("dq_negative_cost").cast("int")).alias("negative_cost_rows"),
    F.sum(F.col("dq_stop_before_start").cast("int")).alias("stop_before_start_rows"))
dq.write.format("delta").mode("overwrite").save(f"{SILVER}/_dq_summary")
display(dq)
