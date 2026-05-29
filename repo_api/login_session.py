import yaml
import requests
import os
import sys

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    yaml_path = os.path.join(script_dir, 'dict.yaml')
    
    if not os.path.exists(yaml_path):
        print(f"Error: {yaml_path} not found.")
        sys.exit(1)
        
    with open(yaml_path, 'r') as f:
        try:
            config = yaml.safe_load(f)
        except Exception as e:
            print(f"Error parsing dict.yaml: {e}")
            sys.exit(1)
            
    login_url = config.get('login_url')
    login_body = config.get('login_body')
    
    if not login_url or not login_body:
        print("Error: login_url or login_body missing in dict.yaml")
        sys.exit(1)
        
    print(f"Logging in to: {login_url}")
    try:
        response = requests.post(login_url, json=login_body)
        response.raise_for_status()
        data = response.json()
        
        session_id = data.get('userInfo', {}).get('sessionId')
        if session_id:
            print("Login successful!")
            print(f"Session ID: {session_id}")
            
            # Write session ID to session_id.txt
            session_file_path = os.path.join(script_dir, 'session_id.txt')
            with open(session_file_path, 'w') as sf:
                sf.write(session_id)
            print(f"Session ID stored in {session_file_path}")
            
            return session_id
        else:
            print("Error: sessionId not found in response.")
            print(f"Response: {data}")
            sys.exit(1)
    except requests.exceptions.RequestException as e:
        print(f"HTTP request failed: {e}")
        if e.response is not None:
            print(f"Response Content: {e.response.text}")
        sys.exit(1)

if __name__ == '__main__':
    main()
