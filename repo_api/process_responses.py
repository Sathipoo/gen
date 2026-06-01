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
    Retrieve value from a nested dictionary or list of dicts using a dot-separated path.
    If the path navigates into a list and is not a specific index, it gathers the values
    from all items in the list and returns them as a comma-separated string.
    """
    parts = path.split('.')
    current = data
    for i, part in enumerate(parts):
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list):
            # Check if part is a numeric index
            try:
                idx = int(part)
                if idx < len(current):
                    current = current[idx]
                else:
                    return None
            except ValueError:
                # It is a key we want to extract from all elements in the list
                remaining_path = ".".join(parts[i:])
                sub_values = []
                for item in current:
                    val = get_nested_val(item, remaining_path)
                    if val is not None:
                        # Avoid nested lists/dicts serialization or handle them cleanly
                        if isinstance(val, (dict, list)):
                            sub_values.append(json.dumps(val))
                        else:
                            sub_values.append(str(val))
                return ", ".join(sub_values) if sub_values else None
        else:
            return None
            
    if isinstance(current, (dict, list)):
        return json.dumps(current)
    return current

def discover_keys_from_record(record, prefix=""):
    """
    Recursively discover all dot-path keys present in a JSON record.
    """
    keys = set()
    if isinstance(record, dict):
        for k, v in record.items():
            path = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                keys.update(discover_keys_from_record(v, path))
            elif isinstance(v, list) and k in ["entries", "subTaskEntries", "transformationEntries"]:
                for item in v:
                    if isinstance(item, dict):
                        keys.update(discover_keys_from_record(item, path))
            else:
                keys.add(path)
    return keys

def resolve_headers(enabled_paths):
    """
    Resolve column headers for each path. If there are collisions (identical key names),
    replaces dots with underscores to ensure unique, clear headers.
    """
    resolved = {}
    
    # 1. Start with the last segment of the path as the default column name
    for path in enabled_paths:
        parts = path.split('.')
        resolved[path] = parts[-1]
        
    # 2. Check for duplicate column names and resolve collisions by replacing dots with underscores
    while True:
        header_counts = {}
        for path, header in resolved.items():
            header_counts[header] = header_counts.get(header, 0) + 1
            
        duplicates = {h for h, count in header_counts.items() if count > 1}
        if not duplicates:
            break  # No duplicate headers remaining
            
        for path, header in list(resolved.items()):
            if header in duplicates:
                new_header = path.replace('.', '_')
                
                # If still collides, append a suffix
                if new_header == header or new_header in resolved.values():
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
    if os.path.exists(responses_dir):
        json_files = [f for f in os.listdir(responses_dir) if f.endswith('.json')]
        json_files.sort()
        if json_files:
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
            
    print(f"Loaded {len(all_raw_data)} primary task records.")
    
    # 3. Discover all unique keys from raw records and update config
    discovered_keys = set()
    for record in all_raw_data:
        discovered_keys.update(discover_keys_from_record(record))
    print(f"Discovered {len(discovered_keys)} unique field paths in the JSON files.")
    
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
        
    # 4. Extract enabled field paths and resolve headers
    enabled_paths = extract_enabled_fields(new_config)
    if not enabled_paths:
        print("Warning: No fields are enabled in fields_config.yaml. Output CSV will be empty.")
        
    header_mapping = resolve_headers(enabled_paths)
    csv_headers = [header_mapping[path] for path in enabled_paths]
    
    print("\nColumns to be written to CSV:")
    for path in enabled_paths:
        print(f"  - {path} -> {header_mapping[path]}")
        
    # 5. Write data to CSV
    csv_path = os.path.join(script_dir, 'activity_logs.csv')
    try:
        with open(csv_path, 'w', newline='', encoding='utf-8') as cf:
            writer = csv.writer(cf)
            writer.writerow(csv_headers)
            
            for record in all_raw_data:
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
