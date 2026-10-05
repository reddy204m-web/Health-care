# Healthcare Usage Analytics (Medallion + GenAI)

ADF -> ADLS Gen2 -> Databricks (Bronze/Silver/Gold, Delta) -> Synapse Serverless SQL -> Power BI, plus a GenAI layer.
Synthetic data only (Synthea). Never use real patient data.

## Run order
1. Create ADLS Gen2 account with container `lake`; generate data (`databricks/05_generate_synthea.md`).
2. Import `databricks/` into a Databricks Repo at `/Repos/healthcare-usage`.
   Free Edition users: set `lake_root` to a Unity Catalog volume path and run notebooks 01 -> 03 manually.
3. Deploy ADF JSON (`adf/`) and trigger `pl_healthcare_medallion` (replace `<storageaccount>` / workspace placeholders).
4. Run `synapse/01_serverless_setup.sql`, then try `02_example_queries.sql`.
5. Power BI Desktop -> Get Data -> Azure Synapse Analytics SQL (serverless endpoint, database `healthcare_gold`);
   add measures from `powerbi/dax_measures.dax`.
6. GenAI: `pip install -r genai/requirements.txt`, set `GEMINI_API_KEY` and `SYNAPSE_CONN`, run `streamlit run genai/app.py`.

## Cost control
Budget alert at $5-10, single-node cluster with 10-min auto-terminate, serverless Synapse only, disable ADF triggers, delete the resource group when done.
