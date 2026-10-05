USE healthcare_gold;
-- Cost by payer
SELECT p.payer_name, COUNT(*) AS encounters, SUM(f.total_claim_cost) AS total_cost
FROM gold.fact_encounters f JOIN gold.dim_payer p ON p.payer_id = f.payer_id
GROUP BY p.payer_name ORDER BY total_cost DESC;

-- 30-day readmission rate by provider (min 20 discharges)
SELECT pr.provider_name, SUM(a.inpatient_discharges) AS discharges,
       CAST(SUM(a.readmissions) AS FLOAT) / NULLIF(SUM(a.inpatient_discharges),0) AS readmission_rate
FROM gold.agg_readmission_30d a JOIN gold.dim_provider pr ON pr.provider_id = a.provider_id
GROUP BY pr.provider_name HAVING SUM(a.inpatient_discharges) >= 20
ORDER BY readmission_rate DESC;

-- Read-only login for the GenAI app
-- CREATE LOGIN genai_reader WITH PASSWORD = '<pwd>';  (run in master)
-- CREATE USER genai_reader FOR LOGIN genai_reader;
-- GRANT SELECT ON SCHEMA::gold TO genai_reader;
-- GRANT REFERENCES ON DATABASE SCOPED CREDENTIAL::lake_msi TO genai_reader;
