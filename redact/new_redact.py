import boto3
import pandas as pd
import json
import logging
import random
import string
import argparse
from io import StringIO
from datetime import datetime
from urllib.parse import urlparse
import os

def generate_random_ssn():
    """Generate a random SSN in format XXX-XX-XXXX"""
    return f"{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(1000, 9999)}"

def generate_random_string(length=10):
    """Generate a random string of specified length"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def parse_s3_path(s3_path):
    """Parse S3 URI into bucket and key"""
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {s3_path}")
    parsed = urlparse(s3_path)
    return parsed.netloc, parsed.path.lstrip('/')

def setup_logger(input_key):
    """Set up logger with dynamic log file name including datetime stamp"""
    base_name = os.path.basename(input_key).split('.')[0]
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"data_redaction_{base_name}_{timestamp}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=filename,
        filemode='w'
    )
    return logging.getLogger(__name__)

import logging
import os
from datetime import datetime

def setup_logger(input_key):
    """
    Set up logger with dynamic log file name including datetime stamp.
    Also prints log to console.
    """
    # Extract base name (file without extension) from the input key
    base_name = os.path.basename(input_key).split('.')[0]
    
    # Generate a timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Define log directory (change "logs" to whatever directory you want)
    log_dir = "logs"
    # Create the directory if not exists
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Construct the full log file path
    log_filename = f"{log_dir.rstrip('/')}/data_redaction_{base_name}_{timestamp}.log"
    
    # Create a custom logger (use __name__ or any other name you like)
    logger = logging.getLogger(__name__)
    
    # Set the minimum logging level for this logger
    logger.setLevel(logging.INFO)
    
    # Create a file handler that writes to log_filename
    file_handler = logging.FileHandler(log_filename, mode='w')
    file_handler.setLevel(logging.INFO)
    
    # Create a console (stream) handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Define a common formatter for both handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Add both handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def redact_data(df, config, logger):
    """Redact data based on configuration"""
    try:
        for column, rules in config['columns'].items():
            if column in df.columns:
                redact_type = rules.get('type', 'default')
                default_value = rules.get('value')

                if redact_type == 'default' and default_value is not None:
                    df[column] = default_value
                    logger.info(f"Redacted column '{column}' with default value: {default_value}")

                elif redact_type == 'random_ssn':
                    df[column] = [generate_random_ssn() for _ in range(len(df))]
                    logger.info(f"Redacted column '{column}' with random SSNs")

                elif redact_type == 'random_string':
                    length = rules.get('length', 10)
                    df[column] = [generate_random_string(length) for _ in range(len(df))]
                    logger.info(f"Redacted column '{column}' with random strings of length {length}")

        return df

    except Exception as e:
        logger.error(f"Error during redaction: {str(e)}")
        raise

def process_file(input_path, config_path):
    """Process S3 file and overwrite with redacted version"""
    input_bucket, input_key = parse_s3_path(input_path)
    config_bucket, config_key = parse_s3_path(config_path)
    
    logger = setup_logger(input_key)
    s3_client = boto3.client('s3')

    try:
        # Read JSON config
        logger.info(f"Reading config from: {config_path}")
        config_obj = s3_client.get_object(Bucket=config_bucket, Key=config_key)
        config = json.loads(config_obj['Body'].read().decode('utf-8'))

        # Read input CSV from S3
        logger.info(f"Reading input file: {input_path}")
        obj = s3_client.get_object(Bucket=input_bucket, Key=input_key)
        df = pd.read_csv(obj['Body'])

        # Redact the data
        redacted_df = redact_data(df, config, logger)

        # Convert to CSV
        csv_buffer = StringIO()
        redacted_df.to_csv(csv_buffer, index=False)

        # Overwrite original file
        logger.info(f"Overwriting original file on S3: {input_path}")
        s3_client.put_object(
            Bucket=input_bucket,
            Key=input_key,
            Body=csv_buffer.getvalue()
        )

        logger.info("Redaction completed and file updated in S3.")
        return f"s3://{input_bucket}/{input_key}"

    except Exception as e:
        logger.error(f"Failed to process file: {str(e)}")
        raise

def main():
    parser = argparse.ArgumentParser(description="Redact sensitive data in a CSV file stored in S3.")
    parser.add_argument('--input_file', required=True, help='Full S3 path to input CSV file (e.g., s3://bucket/key)')
    parser.add_argument('--config_file', required=True, help='Full S3 path to redaction config JSON file (e.g., s3://bucket/key)')

    args = parser.parse_args()

    try:
        result_path = process_file(args.input_file, args.config_file)
        print(f"File successfully redacted and saved at: {result_path}")
    except Exception as e:
        print(f"Redaction failed: {str(e)}")

if __name__ == "__main__":
    main()
