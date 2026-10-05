# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------
def w(df, name):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{GOLD}/{name}")

enc  = spark.read.format("delta").load(f"{SILVER}/encounters")
pat  = spark.read.format("delta").load(f"{SILVER}/patients").filter("is_current")
prov = spark.read.format("delta").load(f"{SILVER}/providers")
pay  = spark.read.format("delta").load(f"{SILVER}/payers")
cond = spark.read.format("delta").load(f"{SILVER}/conditions")

# COMMAND ----------
# DIMENSIONS
dim_date = (spark.sql("SELECT explode(sequence(to_date('2010-01-01'), to_date('2030-12-31'), interval 1 day)) AS date")
    .select(F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"), "date",
            F.year("date").alias("year"), F.quarter("date").alias("quarter"),
            F.month("date").alias("month"), F.date_format("date", "MMMM").alias("month_name"),
            F.date_format("date", "E").alias("weekday")))
w(dim_date, "dim_date")

today = F.current_date()
dim_patient = (pat.withColumn("age", F.floor(F.months_between(F.coalesce("death_date", today), "birth_date") / 12))
    .withColumn("age_group", F.when(F.col("age") < 18, "0-17").when(F.col("age") < 40, "18-39")
                 .when(F.col("age") < 65, "40-64").otherwise("65+"))
    .select("patient_id", "gender", "race", "ethnicity", "city", "state", "age", "age_group", "income",
            F.col("death_date").isNotNull().alias("is_deceased")))
w(dim_patient, "dim_patient")
w(prov, "dim_provider")
w(pay, "dim_payer")

# COMMAND ----------
# READMISSION LOGIC: index inpatient stay followed by another inpatient admission within 30 days
inp = enc.filter("encounter_class = 'inpatient'")
win = Window.partitionBy("patient_id").orderBy("start_ts")
readm = (inp.withColumn("next_start", F.lead("start_ts").over(win))
    .withColumn("readmitted_30d",
        F.when(F.datediff("next_start", "stop_ts").between(0, 30), 1).otherwise(0))
    .select("encounter_id", "readmitted_30d"))

# FACT
fact = (enc.join(readm, "encounter_id", "left")
    .withColumn("readmitted_30d", F.coalesce("readmitted_30d", F.lit(0)))
    .withColumn("start_date_key", F.date_format("start_ts", "yyyyMMdd").cast("int"))
    .withColumn("length_of_stay_days", F.round((F.col("stop_ts").cast("long") - F.col("start_ts").cast("long")) / 86400, 2))
    .withColumn("patient_out_of_pocket", F.col("total_claim_cost") - F.col("payer_coverage"))
    .select("encounter_id", "patient_id", "provider_id", "payer_id", "organization_id", "encounter_class",
            "start_ts", "stop_ts", "start_date_key", "length_of_stay_days", "base_cost",
            "total_claim_cost", "payer_coverage", "patient_out_of_pocket", "reason_desc", "readmitted_30d"))
w(fact, "fact_encounters")

# COMMAND ----------
# AGGREGATES
agg_readm = (fact.filter("encounter_class = 'inpatient'")
    .groupBy("provider_id", F.date_format("stop_ts", "yyyy-MM").alias("discharge_month"))
    .agg(F.count("*").alias("inpatient_discharges"), F.sum("readmitted_30d").alias("readmissions"))
    .withColumn("readmission_rate", F.round(F.col("readmissions") / F.col("inpatient_discharges"), 4)))
w(agg_readm, "agg_readmission_30d")

agg_cond = (cond.join(fact.select("encounter_id", "total_claim_cost"), "encounter_id")
    .groupBy("condition_desc")
    .agg(F.countDistinct("patient_id").alias("patients"), F.countDistinct("encounter_id").alias("encounters"),
         F.round(F.sum("total_claim_cost"), 2).alias("total_cost"),
         F.round(F.avg("total_claim_cost"), 2).alias("avg_cost_per_encounter")))
w(agg_cond, "agg_cost_by_condition")

agg_monthly = (fact.groupBy(F.date_format("start_ts", "yyyy-MM").alias("month"), "encounter_class")
    .agg(F.count("*").alias("encounters"), F.countDistinct("patient_id").alias("patients"),
         F.round(F.sum("total_claim_cost"), 2).alias("total_cost"),
         F.round(F.avg("length_of_stay_days"), 2).alias("avg_los_days")))
w(agg_monthly, "agg_utilization_monthly")
print("Gold model built.")
