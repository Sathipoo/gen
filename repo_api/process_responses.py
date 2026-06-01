import os
import sys
import csv
import json
import yaml

def extract_enabled_fields(config_dict):
    """
    Extract all paths from the flat YAML config that are set to True.
    """
    enabled_fields = []
    for key, value in config_dict.items():
        if value is True:
            enabled_fields.append(key)
    return enabled_fields

def get_nested_val(data, path):
    """
    Retrieve value from a dictionary using a dot-separated path.
    """
    parts = path.split('.')
    current = data
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current

def flatten_activity_log(records, parent_info=None):
    """
    Recursively flatten parent records and nested child tasks (from 'entries' or 'subTaskEntries')
    into a flat list of dictionaries representing individual CSV rows.
    """
    flat_records = []
    if not isinstance(records, list):
        return flat_records
        
    for r in records:
        if not isinstance(r, dict):
            continue
            
        item = {}
        # 1. Copy all top-level keys except list keys that need custom processing
        for k, v in r.items():
            if k not in ["entries", "subTaskEntries", "transformationEntries", "logEntryItemAttrs"]:
                item[k] = v
                
        # 2. Add custom parent traceability fields
        if parent_info:
            item["parent_id"] = parent_info.get("id", "")
            item["parent_objectName"] = parent_info.get("objectName", "")
        else:
            item["parent_id"] = ""
            item["parent_objectName"] = ""
            
        # 3. Flatten logEntryItemAttrs nested keys into dot-notation paths
        attrs = r.get("logEntryItemAttrs", {})
        if isinstance(attrs, dict):
            for attr_k, attr_v in attrs.items():
                item[f"logEntryItemAttrs.{attr_k}"] = attr_v
                
        # 4. Save references to transformationEntries or sessionVariables if they are basic types
        for list_key in ["transformationEntries", "sessionVariables"]:
            list_val = r.get(list_key)
            if list_val is not None:
                item[list_key] = json.dumps(list_val) if not isinstance(list_val, (str, int, float, bool)) else list_val
                
        flat_records.append(item)
        
        # 5. Recursively process nested child tasks in 'entries' or 'subTaskEntries'
        sub_entries = r.get("entries", [])
        if not sub_entries:
            sub_entries = r.get("subTaskEntries", [])
            
        if isinstance(sub_entries, list) and sub_entries:
            current_parent = {
                "id": r.get("id"),
                "objectName": r.get("objectName")
            }
            flat_records.extend(flatten_activity_log(sub_entries, current_parent))
            
    return flat_records

def discover_keys_from_flat_records(flat_records):
    """
    Collect all unique keys/paths present across all flattened records.
    """
    keys = set()
    for r in flat_records:
        for k in r.keys():
            keys.add(k)
    return keys

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
        header_counts = {}
        for path, header in resolved.items():
            header_counts[header] = header_counts.get(header, 0) + 1
            
        duplicates = {h for h, count in header_counts.items() if count > 1}
        if not duplicates:
            break  # No duplicate headers remaining
            
        for path, header in list(resolved.items()):
            if header in duplicates:
                parts = path.split('.')
                if len(parts) > 1:
                    new_header = "_".join(parts[-2:])
                else:
                    new_header = path
                    
                if new_header == header:
                    suffix = 1
                    temp_header = f"{new_header}_{suffix}"
                    while temp_header in resolved.values():
                        suffix += 1
                        temp_header = f"{new_header}_{suffix}"
                    new_header = temp_header
                    
                resolved[path] = new_header
                
    return resolved

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    responses_dir = os.path.join(script_dir, 'responses')
    config_path = os.path.join(script_dir, 'fields_config.yaml')
    sample_record_path = os.path.join(script_dir, 'sample_record.json')
    
    all_raw_data = []
    
    # 1. Look for response files in responses/ folder first
    has_responses = False
    if os.path.exists(responses_dir):
        json_files = [f for f in os.listdir(responses_dir) if f.endswith('.json')]
        json_files.sort()
        if json_files:
            has_responses = True
            print(f"Reading response files from {responses_dir}...")
            for filename in json_files:
                filepath = os.path.join(responses_dir, filename)
                with open(filepath, 'r') as jf:
                    try:
                        data = json.load(jf)
                        if isinstance(data, list):
                            all_raw_data.extend(data)
                        elif isinstance(data, dict):
                            all_raw_data.append(data)
                    except Exception as e:
                        print(f"  - Error reading {filename}: {e}")
                        
    # 2. Fallback to sample_record.json if no response files were loaded
    if not all_raw_data:
        if os.path.exists(sample_record_path):
            print(f"No response files found. Loading fallback data from {sample_record_path}...")
            with open(sample_record_path, 'r') as sf:
                try:
                    data = json.load(sf)
                    if isinstance(data, list):
                        all_raw_data.extend(data)
                    elif isinstance(data, dict):
                        all_raw_data.append(data)
                except Exception as e:
                    print(f"Error reading {sample_record_path}: {e}")
                    sys.exit(1)
        else:
            print(f"Error: No JSON logs found in {responses_dir} and fallback file {sample_record_path} does not exist.")
            sys.exit(1)
            
    # 3. Recursively flatten the log records (including parent relationships for child tasks)
    flat_records = flatten_activity_log(all_raw_data)
    if not flat_records:
        print("No log records found to process.")
        sys.exit(1)
        
    print(f"Successfully loaded and flattened {len(flat_records)} total records (including nested child tasks).")
    
    # 4. Discover all unique keys from flattened records and update config
    discovered_keys = discover_keys_from_flat_records(flat_records)
    print(f"Discovered {len(discovered_keys)} unique field paths.")
    
    existing_config = {}
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            try:
                existing_config = yaml.safe_load(f) or {}
            except Exception as e:
                print(f"Warning: could not parse existing fields_config.yaml: {e}")
                existing_config = {}
                
    # Merge keys: preserve values from existing, add new ones as False
    new_config = {}
    # Use order of existing config keys first
    for k, v in existing_config.items():
        new_config[k] = v
        
    updated = False
    for key in sorted(discovered_keys):
        if key not in new_config:
            new_config[key] = False
            updated = True
            
    if updated:
        with open(config_path, 'w') as f:
            yaml.safe_dump(new_config, f, default_flow_style=False, sort_keys=False)
        print(f"Added newly discovered fields to fields_config.yaml (defaulted to false).")
        
    # 5. Resolve enabled fields and headers
    enabled_paths = extract_enabled_fields(new_config)
    if not enabled_paths:
        print("Warning: No fields are enabled in fields_config.yaml. Output CSV will be empty.")
        
    header_mapping = resolve_headers(enabled_paths)
    csv_headers = [header_mapping[path] for path in enabled_paths]
    
    print("\nColumns to be written to CSV:")
    for path in enabled_paths:
        print(f"  - {path} -> {header_mapping[path]}")
        
    # 6. Write to CSV
    csv_path = os.path.join(script_dir, 'activity_logs.csv')
    try:
        with open(csv_path, 'w', newline='', encoding='utf-8') as cf:
            writer = csv.writer(cf)
            writer.writerow(csv_headers)
            
            for record in flat_records:
                row = []
                for path in enabled_paths:
                    val = get_nested_val(record, path)
                    row.append(val if val is not None else "")
                writer.writerow(row)
                
        print(f"\nSuccess! CSV file created at: {csv_path}")
    except Exception as e:
        print(f"Error writing CSV: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
