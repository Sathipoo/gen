import csv
import os
from collections import Counter
def get_clean_reader(file_handle):
    """Returns a DictReader with cleaned header names."""
    # Read the first line to check for junk/paths
    pos = file_handle.tell()
    first_line = file_handle.readline()
    
    # If the first line looks like a Windows path (common in your screenshots), skip it
    if ":" in first_line and "\\" in first_line and "," not in first_line:
        print(f"--- Skipping metadata line: {first_line.strip()}")
    else:
        file_handle.seek(pos) # Go back to start if it looks like a real header
    reader = csv.DictReader(file_handle)
    # Clean whitespace and BOM from headers
    if reader.fieldnames:
        reader.fieldnames = [hf.strip().replace('\ufeff', '').upper() for hf in reader.fieldnames]
    return reader
def compare_counts(extract_csv: str, reference_csv: str):
    extract_counts = Counter()
    
    # 1. Process Extract File
    if os.path.exists(extract_csv):
        with open(extract_csv, mode='r', encoding='utf-8-sig') as f:
            reader = get_clean_reader(f)
            for row in reader:
                # Use .get() to avoid KeyError if the column is missing
                t_name = row.get('TABLE_NAME')
                if t_name:
                    extract_counts[t_name.upper().strip()] += 1
    
    # 2. Process Reference File
    reference_data = {}
    if os.path.exists(reference_csv):
        with open(reference_csv, mode='r', encoding='utf-8-sig') as f:
            # If your file is space-separated, we try to handle that
            content = f.read()
            # Check if it's actually space or tab separated
            dialect = csv.Sniffer().sniff(content[:2000]) if ',' not in content[:100] else None
            f.seek(0)
            
            if dialect:
                reader = csv.DictReader(f, dialect=dialect)
            else:
                reader = get_clean_reader(f)
                
            if reader.fieldnames:
                reader.fieldnames = [hf.strip().upper() for hf in reader.fieldnames]
            for row in reader:
                t_name = row.get('TABLE_NAME')
                col_val = row.get('COLUMN_COUNT') or row.get('NUMBER OF COLUMNS')
                if t_name and col_val:
                    try:
                        reference_data[t_name.upper().strip()] = int(col_val)
                    except ValueError:
                        continue
    # 3. Report
    print(f"\n{'TABLE_NAME':<35} | {'REF_COUNT':<10} | {'FILE_COUNT':<10} | {'STATUS'}")
    print("-" * 80)
    all_tables = sorted(set(list(extract_counts.keys()) + list(reference_data.keys())))
    for table in all_tables:
        ref = reference_data.get(table, "N/A")
        fil = extract_counts.get(table, "N/A")
        status = "MATCH ✅" if ref == fil else "MISMATCH ❌"
        if ref == "N/A" or fil == "N/A": status = "MISSING"
        print(f"{table:<35} | {str(ref):<10} | {str(fil):<10} | {status}")
if __name__ == "__main__":
    # Update these paths for the other machine
    extract = "GBP_ORACLE_DDL_FILE_EXTRACT.csv"
    reference = "col_count.csv"
    compare_counts(extract, reference)
