import json

def validate_schema(input_json, schema_json):
    for key, schema_value in schema_json.items():
        if key not in input_json:
            print(f"Validation Error: Key '{key}' is missing in input JSON.")
            return False

        input_value = input_json[key]
        schema_type = type(schema_value)
        input_type = type(input_value)

        if schema_type != input_type:
            print(f"Validation Error: Key '{key}' type mismatch. Expected '{schema_type.__name__}', got '{input_type.__name__}'.")
            return False

        if isinstance(schema_value, dict):
            if not validate_schema(input_value, schema_value):
                return False
        elif isinstance(schema_value, list):
            if input_value: # Check if input list is not empty, then validate the first element against schema
                if not isinstance(input_value, list):
                    print(f"Validation Error: Key '{key}' should be a list in input JSON.")
                    return False
                if schema_value: # If schema list is not empty, validate against the first element of schema list
                    for item in input_value: # Validate each item in input list against the schema list's first element
                        if not validate_schema(item, schema_value[0]):
                            return False
            elif schema_value: # schema has list definition but input list is empty. This is acceptable as per structure, can be adjusted based on requirement.
                if not isinstance(input_value, list):
                    print(f"Validation Error: Key '{key}' should be a list in input JSON.")
                    return False


    return True
