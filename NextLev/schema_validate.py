#!/usr/bin/env python3
import json
import sys
import os
from datetime import datetime
from dmv_load_params import get_env_params
import dmv_functions

def apply_dynamic_schema_modifications(input_json, schema_json, logger):
    """
    Dynamically updates the schema based on the input JSON's 'transactionType'.
    - If transactionType is 'CA': cancellationReason becomes required.
    - If transactionType is 'RI': reinstatementReason becomes required.
    """
    transaction_type = input_json.get("transactionType", "").upper()
    logger.info("Applying dynamic modifications for transactionType: %s", transaction_type)
    
    if "cancellationReason" in schema_json:
        if transaction_type == "CA":
            logger.info("Setting cancellationReason as required based on transactionType 'CA'.")
            schema_json["cancellationReason"]["isrequired"] = "required"
        else:
            logger.info("Setting cancellationReason as optional.")
            schema_json["cancellationReason"]["isrequired"] = "optional"

    if "reinstatementReason" in schema_json:
        if transaction_type == "RI":
            logger.info("Setting reinstatementReason as required based on transactionType 'RI'.")
            schema_json["reinstatementReason"]["isrequired"] = "required"
        else:
            logger.info("Setting reinstatementReason as optional.")
            schema_json["reinstatementReason"]["isrequired"] = "optional"
    
    return schema_json

def validate_schema(input_json, schema_json, logger):
    """
    Recursively validates input_json against schema_json.
    Instead of exiting on the first error, it accumulates all errors.
    Returns a list of error messages.
    """
    errors = []
    
    # If schema_json is a primitive descriptor, validate directly.
    if isinstance(schema_json, dict) and "type" in schema_json and set(schema_json.keys()) <= {"type", "isrequired"}:
        schema_type = schema_json["type"].lower()
        if schema_type == "string":
            if not isinstance(input_json, str):
                errors.append(f"Expected type 'string' but got '{type(input_json).__name__}'.")
        elif schema_type == "number":
            if not isinstance(input_json, (int, float)):
                errors.append(f"Expected type 'number' but got '{type(input_json).__name__}'.")
        elif schema_type == "array":
            if not isinstance(input_json, list):
                errors.append(f"Expected type 'array' but got '{type(input_json).__name__}'.")
        else:
            errors.append(f"Unsupported type '{schema_type}'.")
        return errors
    
    # Otherwise, assume schema_json is a mapping of keys to field schemas.
    keys_to_validate = [k for k in schema_json if k not in ("isrequired", "type", "items")]
    for key in keys_to_validate:
        field_schema = schema_json[key]
        is_required = field_schema.get("isrequired") == "required"
        if key not in input_json:
            if is_required:
                errors.append(f"Required key '{key}' is missing.")
            continue  # Skip optional keys that are missing

        input_value = input_json[key]
        if input_value is None:
            if is_required:
                errors.append(f"Required key '{key}' cannot be null.")
            continue

        if "type" in field_schema:
            schema_type = field_schema["type"].lower()
            if schema_type == "string":
                if not isinstance(input_value, str):
                    errors.append(f"Key '{key}' type mismatch: expected 'string', got '{type(input_value).__name__}'.")
            elif schema_type == "number":
                if not isinstance(input_value, (int, float)):
                    errors.append(f"Key '{key}' type mismatch: expected 'number', got '{type(input_value).__name__}'.")
            elif schema_type == "array":
                if not isinstance(input_value, list):
                    errors.append(f"Key '{key}' should be an array.")
                else:
                    if "items" in field_schema and input_value:
                        logger.info("Validating array items for key '%s'.", key)
                        for i, item in enumerate(input_value):
                            logger.info("Validating array item %s for key '%s'.", i, key)
                            item_errors = validate_schema(item, field_schema["items"], logger)
                            if item_errors:
                                for err in item_errors:
                                    errors.append(f"In array '{key}' item {i}: {err}")
            else:
                errors.append(f"Unsupported type '{schema_type}' for key '{key}'.")
        else:
            # Nested object
            if not isinstance(input_value, dict):
                errors.append(f"Key '{key}' should be an object.")
            else:
                nested_errors = validate_schema(input_value, field_schema, logger)
                errors.extend(nested_errors)
    return errors

def main_validate(env_params, input_payload_json, logger):
    try:
        logger.info("Loading input JSON from %s.", input_payload_json)
        with open(input_payload_json, 'r') as f:
            input_json = json.load(f)
        
        policyStatus = input_json['policyStatus']
        if policyStatus.upper() == "I":
            struct_file = env_params['structure_json_file']["I"]
        elif policyStatus.upper() == "B":
            struct_file = env_params['structure_json_file']["B"]
        else:
            logger.error("Unhandled policyStatus: %s", policyStatus)
            raise Exception("Unhandled policyStatus")
        
        logger.info("The policyStatus is %s; loading schema from %s", policyStatus, struct_file)
        with open(struct_file, 'r') as f:
            schema_json = json.load(f)
        
        # Apply dynamic schema modifications based on transactionType.
        schema_json = apply_dynamic_schema_modifications(input_json, schema_json, logger)
        logger.info("Starting schema validation.")
        errors = validate_schema(input_json, schema_json, logger)
        return errors
    except FileNotFoundError as e:
        logger.error("File not found: %s", e)
        raise
    except json.JSONDecodeError as e:
        logger.error("JSON decoding error: %s", e)
        raise
    except Exception as e:
        logger.error("Unexpected error during validation: %s", e)
        raise

if __name__ == "__main__":
    try:
        script_directory = os.path.dirname(os.path.abspath(__file__)).rstrip("/Scripts")
        env_params = get_env_params(script_directory)
    
        input_payload_json_file_name = sys.argv[1]
        input_payload_json = os.path.join(env_params['input_json_path'], input_payload_json_file_name)
        log_file = sys.argv[2]
        log_file_path = os.path.join(env_params['log_dir'], log_file)
    
        logger = dmv_functions.setup_console_logger(__name__, log_file_path)
        logger.info("Script execution started.")
        logger.info("Environment parameters loaded: %s", env_params)
        logger.info("Input JSON file path: %s", input_payload_json)
    
        errors = main_validate(env_params, input_payload_json, logger)
    
        if errors:
            logger.error("The provided JSON does not match the schema. Errors:")
            for err in errors:
                logger.error(" - %s", err)
            raise Exception("Validation failed with errors.")
        else:
            logger.info("The provided JSON matches the schema.")
            
    except IndexError:
        print("Missing command-line argument for input JSON file.")
        raise
    except Exception as e:
        print("Script execution failed.")
        raise
