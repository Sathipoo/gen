import os
import re
import csv
import json
from datetime import datetime

def parse_log_file(file_path):
    filename = os.path.basename(file_path)
    print(f"Parsing log file: {filename}")
    
    with open(file_path, 'r', encoding='utf-8-sig', errors='ignore') as f:
        content = f.read()
        lines = content.splitlines()
        
    # Extract run ID and timestamp from filename
    fn_match = re.match(r'job_log_(\d+)_(\d+)\.txt', filename)
    run_id_fn = fn_match.group(1) if fn_match else ""
    epoch_fn = fn_match.group(2) if fn_match else ""
    
    # Try to parse DBMI Task Config JSON block
    config_data = {}
    start_idx = content.find("DBMI Task Config is ")
    if start_idx != -1:
        json_start = content.find("{", start_idx)
        if json_start != -1:
            brace_count = 0
            json_str = ""
            for char in content[json_start:]:
                json_str += char
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        break
            try:
                config_data = json.loads(json_str)
            except Exception as e:
                print(f"  Warning: JSON config block parsing failed: {e}")

    # Initialize extraction fields
    job_name = ""
    task_name = ""
    run_id = run_id_fn
    source_vendor = ""
    source_host = ""
    source_db = ""
    source_schema = ""
    source_table = ""
    source_query = ""
    target_vendor = ""
    target_host = ""
    target_db = ""
    target_schema = ""
    target_table = ""
    target_query = ""
    row_count = ""
    start_time_str = ""
    end_time_str = ""
    duration_secs = 0.0

    # Populating from JSON config block if available
    if config_data:
        task_id = config_data.get("taskIdentifier", {})
        job_name = task_id.get("jobName", "")
        task_name = task_id.get("taskName", "")
        run_id = task_id.get("runId", run_id)
        
        for subtask in config_data.get("subTasks", []):
            st_type = subtask.get("subTaskType", "")
            ep_type = subtask.get("endpointType", "")
            conn_attrs = subtask.get("connectivityAttributes", {}).get("attributes", {})
            
            if ep_type == "MSSQL_UNLOAD" or st_type == "SOURCE_ENDPOINT":
                source_vendor = conn_attrs.get("unloadDatabaseVendor", "")
                source_host = conn_attrs.get("unloadDatabaseHostName", "").strip()
                source_db = conn_attrs.get("unloadDatabaseIdentifier", "")
                source_schema = conn_attrs.get("unloadDatabaseSchemaName", "")
            elif ep_type == "SNOWFLAKE" or st_type == "TARGET_ENDPOINT":
                target_vendor = conn_attrs.get("writerDatabaseVendor", "")
                target_host = conn_attrs.get("writerDatabaseHostName", "")
                target_db = conn_attrs.get("writerDatabaseIdentifier", "")
                target_schema = conn_attrs.get("snowflakeSchema", "")
                target_table = conn_attrs.get("snowflakeFileName", "")

    # Extract source query from logs if not already populated
    query_match = re.search(r'created Unload query <([^>]+)>', content)
    if query_match:
        source_query = query_match.group(1).strip()
        # Extract DB, Schema, and Table from query as fallback or confirmation
        tbl_match = re.search(r'FROM\s+\[?(\w+)\]?\.\[?(\w+)\]?\.\[?(\w+)\]?', source_query, re.IGNORECASE)
        if tbl_match:
            source_db = tbl_match.group(1)
            source_schema = tbl_match.group(2)
            source_table = tbl_match.group(3)

    # Extract target query (COPY INTO statement)
    copy_match = re.search(r'Executing query:\s*(COPY INTO\s+.*?;)', content, re.DOTALL | re.IGNORECASE)
    if copy_match:
        target_query = re.sub(r'\s+', ' ', copy_match.group(1)).strip()

    # Extract row count
    records_match = re.search(r'Number of records read: (\d+)', content)
    if records_match:
        row_count = records_match.group(1)
    else:
        inserts_match = re.search(r'Inserts: (\d+)', content)
        if inserts_match:
            row_count = inserts_match.group(1)
        else:
            row_count = "0" # Default if not found

    # Extract Start and End timestamps from logs
    # Format typically starts with "YYYY-MM-DD HH:MM:SS,fff"
    ts_pattern = r'^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d{3})'
    
    first_ts = None
    last_ts = None
    
    for line in lines:
        match = re.match(ts_pattern, line)
        if match:
            ts_val = match.group(1)
            if not first_ts:
                first_ts = ts_val
            last_ts = ts_val
            
    if first_ts:
        start_time_str = first_ts
    if last_ts:
        end_time_str = last_ts
        
    # Calculate duration
    if first_ts and last_ts:
        try:
            fmt = "%Y-%m-%d %H:%M:%S,%f"
            t_start = datetime.strptime(first_ts, fmt)
            t_end = datetime.strptime(last_ts, fmt)
            duration_secs = (t_end - t_start).total_seconds()
        except Exception as e:
            print(f"  Warning: Timestamp parsing error: {e}")

    return {
        "Log_File": filename,
        "Run_ID": run_id,
        "Job_Name": job_name,
        "Task_Name": task_name,
        "Source_Vendor": source_vendor,
        "Source_Host": source_host,
        "Source_DB": source_db,
        "Source_Schema": source_schema,
        "Source_Table": source_table,
        "Source_Query": source_query,
        "Target_Vendor": target_vendor,
        "Target_Host": target_host,
        "Target_DB": target_db,
        "Target_Schema": target_schema,
        "Target_Table": target_table,
        "Target_Query": target_query,
        "Count_of_Rows": row_count,
        "Start_Time": start_time_str,
        "End_Time": end_time_str,
        "Duration_Secs": duration_secs
    }

def main():
    log_dir = '/Users/sathishkumardm/Pikachooz2.0/notsoimp/task2'
    output_csv = os.path.join(log_dir, 'extracted_task_details.csv')
    
    files = [f for f in os.listdir(log_dir) if f.startswith('job_log_') and f.endswith('.txt')]
    
    if not files:
        print("No log files matching 'job_log_*.txt' found in task2 directory.")
        return
        
    results = []
    for filename in sorted(files):
        path = os.path.join(log_dir, filename)
        row_data = parse_log_file(path)
        results.append(row_data)
        
    # Write to CSV
    headers = [
        "Log_File", "Run_ID", "Job_Name", "Task_Name", 
        "Source_Vendor", "Source_Host", "Source_DB", "Source_Schema", "Source_Table", "Source_Query",
        "Target_Vendor", "Target_Host", "Target_DB", "Target_Schema", "Target_Table", "Target_Query",
        "Count_of_Rows", "Start_Time", "End_Time", "Duration_Secs"
    ]
    
    with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        for row in results:
            writer.writerow(row)
            
    print(f"\nSuccessfully wrote extraction results to: {output_csv}\n")
    
    # Print a summary table
    print(f"{'Log File':<35} | {'Run ID':<8} | {'Source Table':<25} | {'Target Table':<30} | {'Rows':<8} | {'Duration (s)':<12}")
    print("-" * 125)
    for r in results:
        src_tbl = f"{r['Source_Schema']}.{r['Source_Table']}"
        tgt_tbl = f"{r['Target_Schema']}.{r['Target_Table']}"
        print(f"{r['Log_File']:<35} | {r['Run_ID']:<8} | {src_tbl:<25} | {tgt_tbl:<30} | {r['Count_of_Rows']:<8} | {r['Duration_Secs']:<12.1f}")

if __name__ == '__main__':
    main()
