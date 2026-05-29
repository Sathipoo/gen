import os
import sys
import csv
import json
import yaml

def extract_enabled_fields(config_dict, prefix=""):
    """
    Recursively extract all paths from the YAML config that are set to True.
    """
    enabled_fields = []
    for key, value in config_dict.items():
        current_path = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            enabled_fields.extend(extract_enabled_fields(value, current_path))
        elif value is True:
            enabled_fields.append(current_path)
    return enabled_fields

def get_nested_val(data, path):
    """
    Retrieve value from a nested dictionary using a dot-separated path.
    """
    parts = path.split('.')
    current = data
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current

def collect_all_entries(records):
    """
    Recursively collect all log entries including sub-task entries from the 'entries' list.
    """
    all_entries = []
    if not isinstance(records, list):
        return all_entries
        
    for r in records:
        if not isinstance(r, dict):
            continue
        all_entries.append(r)
        
        # Check for nested sub-task entries in the 'entries' array
        sub_entries = r.get('entries', [])
        if isinstance(sub_entries, list) and sub_entries:
            all_entries.extend(collect_all_entries(sub_entries))
            
    return all_entries

def resolve_headers(enabled_paths):
    """
    Resolve column headers for each path. If there are collisions,
    automatically rename columns to avoid duplicates.
    """
    resolved = {}
    
    # 1. Start with the last segment of the path as the default column name
    for path in enabled_paths:
        parts = path.split('.')
        resolved[path] = parts[-1]
        
    # 2. Check for duplicate column names and resolve collisions
    while True:
        # Find duplicates
        header_counts = {}
        for path, header in resolved.items():
            header_counts[header] = header_counts.get(header, 0) + 1
            
        duplicates = {h for h, count in header_counts.items() if count > 1}
        if not duplicates:
            break  # No duplicate headers remaining
            
        # Resolve duplicates by prefixing with parent key names or adding suffix
        for path, header in list(resolved.items()):
            if header in duplicates:
                parts = path.split('.')
                if len(parts) > 1:
                    # Try using parent prefix, e.g. parent_child
                    new_header = "_".join(parts[-2:])
                else:
                    new_header = path
                    
                # If it still matches the old header or we have another collision, append a suffix
                if new_header == header:
                    suffix = 1
                    temp_header = f"{new_header}_{suffix}"
                    # Find a unique suffix
                    while temp_header in resolved.values():
                        suffix += 1
                        temp_header = f"{new_header}_{suffix}"
                    new_header = temp_header
                    
                resolved[path] = new_header
                
    return resolved

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 1. Load fields_config.yaml
    config_path = os.path.join(script_dir, 'fields_config.yaml')
    if not os.path.exists(config_path):
        print(f"Error: Configuration file not found at {config_path}")
        sys.exit(1)
        
    with open(config_path, 'r') as f:
        try:
            config = yaml.safe_load(f)
        except Exception as e:
            print(f"Error parsing fields_config.yaml: {e}")
            sys.exit(1)
            
    # 2. Extract enabled field paths and resolve headers
    enabled_paths = extract_enabled_fields(config)
    if not enabled_paths:
        print("Error: No fields are enabled in fields_config.yaml.")
        sys.exit(1)
        
    header_mapping = resolve_headers(enabled_paths)
    csv_headers = [header_mapping[path] for path in enabled_paths]
    
    print("Columns to be written to CSV:")
    for path in enabled_paths:
        print(f"  - {path} -> {header_mapping[path]}")
        
    # 3. Read JSON responses from responses/ directory
    responses_dir = os.path.join(script_dir, 'responses')
    if not os.path.exists(responses_dir):
        print(f"Error: Responses directory not found at {responses_dir}")
        sys.exit(1)
        
    all_records = []
    json_files = [f for f in os.listdir(responses_dir) if f.endswith('.json')]
    json_files.sort()  # Process in order
    
    if not json_files:
        print(f"No JSON response files found in {responses_dir}")
        sys.exit(1)
        
    print(f"Processing {len(json_files)} JSON response files...")
    
    for filename in json_files:
        filepath = os.path.join(responses_dir, filename)
        with open(filepath, 'r') as jf:
            try:
                data = json.load(jf)
                # Each file contains a list of records
                flat_entries = collect_all_entries(data)
                all_records.extend(flat_entries)
                print(f"  - {filename}: parsed {len(flat_entries)} entries (including sub-tasks)")
            except Exception as e:
                print(f"  - Error reading {filename}: {e}")
                
    if not all_records:
        print("No log records found to process.")
        sys.exit(1)
        
    # 4. Write data to CSV
    csv_path = os.path.join(script_dir, 'activity_logs.csv')
    try:
        with open(csv_path, 'w', newline='', encoding='utf-8') as cf:
            writer = csv.writer(cf)
            # Write headers
            writer.writerow(csv_headers)
            
            # Write data rows
            for record in all_records:
                row = []
                for path in enabled_paths:
                    val = get_nested_val(record, path)
                    row.append(val if val is not None else "")
                writer.writerow(row)
                
        print(f"\nSuccess! Successfully processed {len(all_records)} total records.")
        print(f"CSV file created at: {csv_path}")
    except Exception as e:
        print(f"Error writing CSV: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
