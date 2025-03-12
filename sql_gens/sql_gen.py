import yaml
import json
from typing import Dict, List

# Load YAML mapping
with open('mapping.yaml', 'r') as yaml_file:
    mapping = yaml.safe_load(yaml_file)

# Load JSON payload (assuming it's saved as 'input.json')
with open('input.json', 'r') as json_file:
    data = json.load(json_file)

def generate_insert_statements(mapping: Dict, data: Dict) -> List[str]:
    insert_statements = []

    # Handle DMV_POLICY
    policy_data = {k: data.get(v['json_path'], None) for k, v in mapping['DMV_POLICY']['fields'].items()}
    policy_columns = ', '.join(policy_data.keys())
    policy_values = ', '.join([f"'{v}'" if v else 'NULL' for v in policy_data.values()])
    insert_statements.append(
        f"INSERT INTO DMV_POLICY ({policy_columns}) VALUES ({policy_values});"
    )

    # Handle DMV_VEHICLE (join vehicleList and registrantList)
    vehicle_list = data.get('vehicleList', [])
    registrant_list = data.get('registrantList', [])
    registrant_map = {r['registrantId']: r for r in registrant_list}

    for vehicle in vehicle_list:
        vehicle_data = {k: vehicle.get(v['json_path'].replace('vehicleList[*].', ''), None)
                       for k, v in mapping['DMV_VEHICLE']['fields'].items()
                       if k != 'FULL_NAME'}
        registrant_id = vehicle.get('registrantId')
        if registrant_id and registrant_id in registrant_map:
            registrant = registrant_map[registrant_id]
            # Concatenate full name if needed (adjust based on your preference)
            full_name = registrant.get('businessName', '')
            vehicle_data['FULL_NAME'] = full_name if full_name else None

        vehicle_columns = ', '.join(vehicle_data.keys())
        vehicle_values = ', '.join([f"'{v}'" if v else 'NULL' for v in vehicle_data.values()])
        insert_statements.append(
            f"INSERT INTO DMV_VEHICLE ({vehicle_columns}) VALUES ({vehicle_values});"
        )

    # Handle DMV_DRIVER
    driver_list = data.get('registrantList', [])
    for driver in driver_list:
        driver_data = {k: driver.get(v['json_path'].replace('registrantList[*].', ''), None)
                      for k, v in mapping['DMV_DRIVER']['fields'].items()}
        # Concatenate full name if needed
        full_name = driver.get('businessName', '')
        driver_data['FULL_NAME'] = full_name if full_name else None
        driver_columns = ', '.join(driver_data.keys())
        driver_values = ', '.join([f"'{v}'" if v else 'NULL' for v in driver_data.values()])
        insert_statements.append(
            f"INSERT INTO DMV_DRIVER ({driver_columns}) VALUES ({driver_values});"
        )

    return insert_statements

# Generate and print insert statements
inserts = generate_insert_statements(mapping, data)
for stmt in inserts:
    print(stmt)

# Save to file if needed
with open('insert_statements.sql', 'w') as sql_file:
    sql_file.write('\n'.join(inserts))