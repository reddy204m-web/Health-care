"""
Natural-language Q&A over the Gold layer (Synapse serverless SQL) using Gemini's free tier.

Guardrails:
  * the prompt contains the SCHEMA ONLY, never patient rows
  * generated SQL is validated: single SELECT, no DDL/DML, no system objects
  * connect with a read-only SQL login (see synapse/02_example_queries.sql)
  * only a small aggregated result is sent back to the LLM for narration
"""
import os, re, json, requests, pyodbc

GEMINI_KEY = os.environ["GEMINI_API_KEY"]
MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")   # check AI Studio for the current free model
CONN = os.environ["SYNAPSE_CONN"]
# e.g. Driver={ODBC Driver 18 for SQL Server};Server=<ws>-ondemand.sql.azuresynapse.net;
#      Database=healthcare_gold;Uid=genai_reader;Pwd=...;Encrypt=yes;

SCHEMA = """
Synapse serverless SQL (T-SQL). Use TOP, not LIMIT. Schema gold:
fact_encounters(encounter_id, patient_id, provider_id, payer_id, organization_id,
  encounter_class /* inpatient|emergency|outpatient|ambulatory|urgentcare|wellness */,
  start_ts, stop_ts, start_date_key /*yyyymmdd*/, length_of_stay_days, base_cost,
  total_claim_cost, payer_coverage, patient_out_of_pocket, reason_desc, readmitted_30d /*0|1*/)
dim_patient(patient_id, gender, race, ethnicity, city, state, age, age_group, income, is_deceased)
dim_provider(provider_id, organization_id, provider_name, specialty, city, state)
dim_payer(payer_id, payer_name)
dim_date(date_key, date, year, quarter, month, month_name, weekday)
agg_readmission_30d(provider_id, discharge_month, inpatient_discharges, readmissions, readmission_rate)
agg_cost_by_condition(condition_desc, patients, encounters, total_cost, avg_cost_per_encounter)
agg_utilization_monthly(month, encounter_class, encounters, patients, total_cost, avg_los_days)
Always reference tables as gold.<table>.
"""

FORBIDDEN = re.compile(
    r"\b(insert|update|delete|drop|alter|create|truncate|merge|exec|execute|grant|revoke|openrowset|xp_|sp_)\b", re.I)

def gemini(prompt: str) -> str:
    r = requests.post(
        f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL}:generateContent",
        headers={"x-goog-api-key": GEMINI_KEY, "Content-Type": "application/json"},
        json={"contents": [{"parts": [{"text": prompt}]}]}, timeout=60)
    r.raise_for_status()
    return r.json()["candidates"][0]["content"]["parts"][0]["text"]

def generate_sql(question: str) -> str:
    prompt = (f"{SCHEMA}\nWrite ONE T-SQL SELECT statement answering the question. "
              f"Return only the SQL, no markdown, no explanation.\nQuestion: {question}")
    sql = re.sub(r"^```(?:sql)?|```$", "", gemini(prompt).strip(), flags=re.M).strip().rstrip(";")
    return sql

def validate(sql: str) -> str:
    if ";" in sql:
        raise ValueError("Multiple statements are not allowed.")
    if not re.match(r"^\s*(with|select)\b", sql, re.I) or FORBIDDEN.search(sql):
        raise ValueError("Only read-only SELECT queries are allowed.")
    if not re.search(r"\btop\s+\d+", sql, re.I) and not re.search(r"\bgroup by\b", sql, re.I):
        sql = re.sub(r"^\s*select\b", "SELECT TOP 100", sql, count=1, flags=re.I)
    return sql

def run(sql: str):
    with pyodbc.connect(CONN, timeout=30) as cn:
        cur = cn.cursor()
        cur.execute(sql)
        cols = [c[0] for c in cur.description]
        return cols, [list(r) for r in cur.fetchmany(200)]

def ask(question: str) -> dict:
    sql = validate(generate_sql(question))
    cols, rows = run(sql)
    narration = gemini(
        f"Question: {question}\nSQL result columns: {cols}\nRows (max 25): {json.dumps(rows[:25], default=str)}\n"
        "Answer in 2-3 plain sentences using only these numbers. This is synthetic data.")
    return {"sql": sql, "columns": cols, "rows": rows, "answer": narration}

if __name__ == "__main__":
    while True:
        q = input("\nAsk a question (blank to quit): ").strip()
        if not q:
            break
        try:
            out = ask(q)
            print("\nSQL:\n", out["sql"], "\n\nAnswer:\n", out["answer"])
        except Exception as ex:
            print("Error:", ex)
