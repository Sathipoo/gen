# IDMC Mass Ingestion Log Analysis (Task 2)

This document details the analysis and extraction of metadata from the IDMC Database Mass Ingestion (DBMI) log files located in this directory.

---

## 1. Automated Extraction
A Python script [analyze_logs.py](file:///Users/sathishkumardm/Pikachooz2.0/notsoimp/task2/analyze_logs.py) has been created to programmatically parse all `job_log_*.txt` files and generate a structured CSV output:
* **Output File**: [extracted_task_details.csv](file:///Users/sathishkumardm/Pikachooz2.0/notsoimp/task2/extracted_task_details.csv)

### Extracted Metadata Summary Table
| Log File Name | Run ID | Source Table | Target Table | Row Count | Duration |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `job_log_156801_1783976658433.txt` | 156801 | `dbo.Account` | `TMA_WORKFLOW.STG_ACCOUNT` | 431,743 | 29.9s |
| `job_log_156802_1783998247987.txt` | 156802 | `dbo.Address` | `TMA_WORKFLOW.STG_ADDRESS` | 193,255 | 27.0s |
| `job_log_156844_1783976893925.txt` | 156844 | `dbo.AccountCustom` | `TMA_WORKFLOW.STG_ACCOUNTCUSTOM` | 280 | 25.4s |
| `job_log_156862_1783998225432.txt` | 156862 | `dbo.AdditionalNotification` | `TMA_WORKFLOW.STG_ADDITIONALNOTIFICATION` | 353 | 26.7s |

---

## 2. Filename Correspondence Analysis
The log filenames follow the structure:
`job_log_<RunID>_<EpochMillis>.txt`

* **`<RunID>`**: The first numeric value is the **Job Run ID** of the Database Ingestion task execution (which matches the `"runId"` attribute inside the configuration dump in each log).
* **`<EpochMillis>`**: The second numeric value is a **Unix epoch timestamp in milliseconds** indicating when the log file was exported/downloaded.
  * **Group A** (`156801`, `156844`): Finalized/exported on **Monday, July 13, 2026, around 21:04-21:08 UTC**.
  * **Group B** (`156802`, `156862`): Finalized/exported on **Tuesday, July 14, 2026, around 03:03-03:04 UTC**.
  * **Note**: Even though logs were exported at different times (Group A vs. Group B), all four tasks originally ran concurrently on **July 12, 2026, between 10:04 AM and 10:06 AM America/Detroit time** (local secure agent timezone).

---

## 3. Technology Stack & Loading Mechanism
* **Source System**: Microsoft SQL Server (MSSQL) on AWS RDS (`tmadbrds001awsp.cz3wdqlbpjmr.us-east-2.rds.amazonaws.com`).
* **Target System**: Snowflake Data Warehouse (`CMA_STG.TMA_WORKFLOW`).
* **Loading Pattern**: **Full Load (Truncate & Load)**.
  1. The DBMI secure agent unloads data from MSSQL to a local temporary CSV file (`STG_<Table>_<Timestamp>.csv`) in the agent workspace.
  2. The agent uploads the CSV file to Snowflake's internal stage `@TMA_WORKFLOW.SF_TMA_WORKFLOW` using a `PUT` command.
  3. It truncates the target table (`TRUNCATE TABLE "TMA_WORKFLOW"."STG_<Table>"`) and loads the staged file using a `COPY INTO` query.
  4. Finally, both the local temporary file and staged file are deleted.

---

## 4. Key Performance Observations
* **High Connection Overhead**: The base execution time for any table (even with minimal rows) is approximately **25 seconds**. This is caused by the initialization of the secure agent task framework, opening/closing DB connections, truncating target tables, and staged file operations.
* **Bulk Load Efficiency**: Bulk copying **431,743 rows** (`Account` table) took only **4.5 seconds** longer than copying **280 rows** (`AccountCustom`), showcasing the efficiency of Snowflake's bulk ingestion stage.
* **Event Handler Scaling**: All jobs had `Event Handler Scaling` disabled because the individual tables (ranging in kilobytes) fell far below the 10GB scaling threshold.
