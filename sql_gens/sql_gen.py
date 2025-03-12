import yaml
import json
from typing import Dict, List

# Load YAML mapping
with open('mapping.yaml', 'r') as yaml_file:
    mapping = yaml.safe_load(yaml_file)

# Load JSON payload (assuming it's saved as 'input.json')
with open('input.json', 'r') as json_file:
    data = json.load(json_file)

def generate_policy_insert(mapping: Dict, data: Dict) -> List[str]:
    """Generate INSERT statements for DMV_POLICY table."""
    insert_statements = []
    policy_data = {k: data.get(v['json_path'], None) for k, v in mapping['DMV_POLICY']['fields'].items()}
    policy_columns = ', '.join(policy_data.keys())
    policy_values = ', '.join([f"'{v}'" if v else 'NULL' for v in policy_data.values()])
    insert_statements.append(
        f"INSERT INTO DMV_POLICY ({policy_columns}) VALUES ({policy_values});"
    )
    return insert_statements

def generate_vehicle_insert(mapping: Dict, data: Dict) -> List[str]:
    """Generate INSERT statements for DMV_VEHICLE table, joining vehicleList and registrantList."""
    insert_statements = []
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
            # Use businessName as a proxy for FULL_NAME
            full_name = registrant.get('businessName', '')
            vehicle_data['FULL_NAME'] = full_name if full_name else None

        vehicle_columns = ', '.join(vehicle_data.keys())
        vehicle_values = ', '.join([f"'{v}'" if v else 'NULL' for v in vehicle_data.values()])
        insert_statements.append(
            f"INSERT INTO DMV_VEHICLE ({vehicle_columns}) VALUES ({vehicle_values});"
        )
    return insert_statements

def generate_driver_insert(mapping: Dict, data: Dict) -> List[str]:
    """Generate INSERT statements for DMV_DRIVER table."""
    insert_statements = []
    driver_list = data.get('registrantList', [])

    for driver in driver_list:
        driver_data = {k: driver.get(v['json_path'].replace('registrantList[*].', ''), None)
                      for k, v in mapping['DMV_DRIVER']['fields'].items()}
        # Use businessName as a proxy for FULL_NAME
        full_name = driver.get('businessName', '')
        driver_data['FULL_NAME'] = full_name if full_name else None
        driver_columns = ', '.join(driver_data.keys())
        driver_values = ', '.join([f"'{v}'" if v else 'NULL' for v in driver_data.values()])
        insert_statements.append(
            f"INSERT INTO DMV_DRIVER ({driver_columns}) VALUES ({driver_values});"
        )
    return insert_statements

# Generate and print insert statements for all tables

policy_inserts = generate_policy_insert(mapping, data)
vehicle_inserts = generate_vehicle_insert(mapping, data)
driver_inserts = generate_driver_insert(mapping, data)

print(policy_inserts)
print(vehicle_inserts)
print(driver_inserts)

# Save to file if needed
with open('insert_statements.sql', 'w') as sql_file:
    sql_file.write('\n'.join(policy_inserts + vehicle_inserts + driver_inserts))


