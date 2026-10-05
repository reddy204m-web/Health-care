-- Run in Synapse Studio against the SERVERLESS (built-in) SQL pool.
-- Prereq: grant the Synapse workspace managed identity (and yourself)
-- "Storage Blob Data Reader" on the storage account.
CREATE DATABASE healthcare_gold;
GO
USE healthcare_gold;
GO
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<StrongPassword!123>';
GO
CREATE DATABASE SCOPED CREDENTIAL lake_msi WITH IDENTITY = 'Managed Identity';
GO
CREATE EXTERNAL DATA SOURCE lake_gold WITH (
    LOCATION   = 'https://<storageaccount>.dfs.core.windows.net/lake/gold',
    CREDENTIAL = lake_msi
);
GO
CREATE SCHEMA gold;
GO

-- One view per Gold Delta table
CREATE VIEW gold.fact_encounters AS
SELECT * FROM OPENROWSET(BULK 'fact_encounters', DATA_SOURCE = 'lake_gold', FORMAT = 'DELTA') AS r;
GO
CREATE VIEW gold.dim_patient AS
SELECT * FROM OPENROWSET(BULK 'dim_patient', DATA_SOURCE = 'lake_gold', FORMAT = 'DELTA') AS r;
GO
CREATE VIEW gold.dim_provider AS
SELECT * FROM OPENROWSET(BULK 'dim_provider', DATA_SOURCE = 'lake_gold', FORMAT = 'DELTA') AS r;
GO
CREATE VIEW gold.dim_payer AS
SELECT * FROM OPENROWSET(BULK 'dim_payer', DATA_SOURCE = 'lake_gold', FORMAT = 'DELTA') AS r;
GO
CREATE VIEW gold.dim_date AS
SELECT * FROM OPENROWSET(BULK 'dim_date', DATA_SOURCE = 'lake_gold', FORMAT = 'DELTA') AS r;
GO
CREATE VIEW gold.agg_readmission_30d AS
SELECT * FROM OPENROWSET(BULK 'agg_readmission_30d', DATA_SOURCE = 'lake_gold', FORMAT = 'DELTA') AS r;
GO
CREATE VIEW gold.agg_cost_by_condition AS
SELECT * FROM OPENROWSET(BULK 'agg_cost_by_condition', DATA_SOURCE = 'lake_gold', FORMAT = 'DELTA') AS r;
GO
CREATE VIEW gold.agg_utilization_monthly AS
SELECT * FROM OPENROWSET(BULK 'agg_utilization_monthly', DATA_SOURCE = 'lake_gold', FORMAT = 'DELTA') AS r;
GO
CREATE VIEW gold.exec_summary AS
SELECT * FROM OPENROWSET(BULK 'exec_summary', DATA_SOURCE = 'lake_gold', FORMAT = 'DELTA') AS r;
GO
