import os
import sys
import json
import yaml
import requests
import datetime
from urllib.parse import urlparse, parse_qs

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 1. Load dict.yaml
    yaml_path = os.path.join(script_dir, 'dict.yaml')
    if not os.path.exists(yaml_path):
        print(f"Error: YAML config file not found at {yaml_path}")
        sys.exit(1)
        
    with open(yaml_path, 'r') as f:
        try:
            config = yaml.safe_load(f)
        except Exception as e:
            print(f"Error parsing YAML: {e}")
            sys.exit(1)
            
    request_url_raw = config.get('request_url')
    if not request_url_raw:
        print("Error: request_url missing in dict.yaml")
        sys.exit(1)
        
    # 2. Read session ID
    session_file_path = os.path.join(script_dir, 'session_id.txt')
    if not os.path.exists(session_file_path):
        print(f"Error: Session file not found at {session_file_path}. Please run login_session.py first.")
        sys.exit(1)
        
    with open(session_file_path, 'r') as sf:
        session_id = sf.read().strip()
        
    if not session_id:
        print("Error: Session ID is empty in session_id.txt. Please login again.")
        sys.exit(1)
        
    # 3. Create responses directory
    responses_dir = os.path.join(script_dir, 'responses')
    os.makedirs(responses_dir, exist_ok=True)
    
    # 4. Parse request URL and parameters
    try:
        parsed_url = urlparse(request_url_raw)
        query_params = parse_qs(parsed_url.query)
        
        # Use values from query parameter or set defaults
        initial_offset = int(query_params.get('offset', [0])[0])
        row_limit = int(query_params.get('rowLimit', [1000])[0])
        
        # Reconstruct base request URL without query string
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
    except Exception as e:
        print(f"Error parsing request_url: {e}")
        sys.exit(1)
        
    # 5. Set headers
    headers = {
        'Accept': config.get('Accept', 'application/json'),
        'Content-Type': config.get('Content-Type', 'application/json'),
        'IDS-SESSION-ID': session_id
    }
    
    offset = initial_offset
    metadata = []
    metadata_path = os.path.join(script_dir, 'metadata.json')
    
    print(f"Starting fetch log loop. Base URL: {base_url}")
    print(f"Row limit per page: {row_limit}")
    
    while True:
        params = {
            'offset': offset,
            'rowLimit': row_limit
        }
        
        print(f"Fetching logs at offset {offset}...")
        
        entry = {
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
            "offset": offset,
            "row_limit": row_limit,
            "count": 0,
            "status": "pending"
        }
        
        try:
            response = requests.get(base_url, headers=headers, params=params)
            
            # Handle non-200 responses
            if response.status_code != 200:
                print(f"Received status code {response.status_code} at offset {offset}.")
                entry["status"] = f"error_{response.status_code}"
                entry["error_message"] = response.text
                metadata.append(entry)
                
                # Write metadata before exiting
                with open(metadata_path, 'w') as mf:
                    json.dump(metadata, mf, indent=2)
                    
                break
                
            response_json = response.json()
            
            # Save the raw JSON response
            response_filename = f"response_offset_{offset}.json"
            response_filepath = os.path.join(responses_dir, response_filename)
            with open(response_filepath, 'w') as rf:
                json.dump(response_json, rf, indent=2)
                
            # If response is a list, count elements. Otherwise it might be dict or error response
            if isinstance(response_json, list):
                count = len(response_json)
                entry["status"] = "success"
                entry["count"] = count
                print(f"Successfully retrieved {count} log entries.")
            else:
                count = 0
                entry["status"] = "unexpected_format"
                entry["error_message"] = "Response is not a JSON list"
                print("Warning: Response is not a JSON list.")
                
            metadata.append(entry)
            
            # Save metadata incrementally
            with open(metadata_path, 'w') as mf:
                json.dump(metadata, mf, indent=2)
                
            # Exit loop if count is less than row_limit or 0
            if count < row_limit or count == 0:
                print(f"Terminating pagination loop: retrieved count {count} is less than rowLimit {row_limit}.")
                break
                
            # Increment offset
            offset += row_limit
            
        except requests.exceptions.RequestException as e:
            print(f"Request failed at offset {offset}: {e}")
            entry["status"] = "failed"
            entry["error_message"] = str(e)
            metadata.append(entry)
            
            with open(metadata_path, 'w') as mf:
                json.dump(metadata, mf, indent=2)
            break
            
    print("Done! Metadata saved to metadata.json")

if __name__ == '__main__':
    main()
