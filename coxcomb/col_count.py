import csv
import os
from collections import Counter

def compare_counts(extract_csv: str, reference_csv: str):
    # 1. Get counts from the Oracle DDL Extract File
    extract_counts = Counter()
    if not os.path.exists(extract_csv):
        print(f"Error: {extract_csv} not found.")
        return

    with open(extract_csv, mode='r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            table_name = row['TABLE_NAME'].upper().strip()
            extract_counts[table_name] += 1

    # 2. Get counts from the Reference CSV (col_count.csv)
    reference_data = {}
    if not os.path.exists(reference_csv):
        print(f"Error: {reference_csv} not found.")
        return

    with open(reference_csv, mode='r', encoding='utf-8-sig') as f:
        # Assuming the reference file is CSV with headers
        reader = csv.DictReader(f)
        for row in reader:
            table_name = row['TABLE_NAME'].upper().strip()
            try:
                reference_data[table_name] = int(row['COLUMN_COUNT'])
            except (ValueError, KeyError):
                continue

    # 3. Perform Comparison
    print(f"{'TABLE_NAME':<35} | {'REF_COUNT':<10} | {'FILE_COUNT':<10} | {'STATUS'}")
    print("-" * 75)

    # Sort by table name for better readability
    all_tables = sorted(set(list(extract_counts.keys()) + list(reference_data.keys())))

    for table in all_tables:
        ref_count = reference_data.get(table, "N/A")
        file_count = extract_counts.get(table, "N/A")
        
        if ref_count == "N/A" or file_count == "N/A":
            status = "MISSING"
        elif ref_count == file_count:
            status = "MATCH ✅"
        else:
            status = "MISMATCH ❌"
            
        print(f"{table:<35} | {str(ref_count):<10} | {str(file_count):<10} | {status}")

if __name__ == "__main__":
    extract_file = "GBP_ORACLE_DDL_FILE_EXTRACT.csv"
    reference_file = "col_count.csv"
    
    print("Column Count Comparison Report")
    print("=" * 75)
    compare_counts(extract_file, reference_file)
