# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------
# Automated executive summary. Only AGGREGATED KPIs leave the workspace, never patient rows.
import requests, json, datetime

API_KEY = dbutils.secrets.get("genai", "gemini-key")   # create a secret scope "genai" with your key
MODEL = "gemini-2.0-flash"                              # check Google AI Studio for the current free-tier model name

monthly = spark.read.format("delta").load(f"{GOLD}/agg_utilization_monthly").orderBy(F.desc("month")).limit(24)
top_cond = spark.read.format("delta").load(f"{GOLD}/agg_cost_by_condition").orderBy(F.desc("total_cost")).limit(10)
readm = (spark.read.format("delta").load(f"{GOLD}/agg_readmission_30d")
         .agg(F.sum("readmissions").alias("r"), F.sum("inpatient_discharges").alias("d")).first())

kpis = {"monthly_utilization": [r.asDict() for r in monthly.collect()],
        "top_conditions_by_cost": [r.asDict() for r in top_cond.collect()],
        "overall_30d_readmission_rate": round((readm.r or 0) / max(readm.d or 1, 1), 4)}

prompt = ("You are a healthcare analytics lead. Using ONLY these aggregated KPIs (synthetic data), write a "
          "concise executive summary: 3 key trends, 2 risks, 2 recommended actions. Do not invent numbers.\n"
          + json.dumps(kpis, default=str))

resp = requests.post(
    f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL}:generateContent",
    headers={"x-goog-api-key": API_KEY, "Content-Type": "application/json"},
    json={"contents": [{"parts": [{"text": prompt}]}]}, timeout=60)
resp.raise_for_status()
summary = resp.json()["candidates"][0]["content"]["parts"][0]["text"]

(spark.createDataFrame([(datetime.datetime.utcnow(), MODEL, summary)],
                       "generated_at timestamp, model string, summary string")
      .write.format("delta").mode("append").save(f"{GOLD}/exec_summary"))
print(summary)
