import boto3
import pandas as pd
import yaml
import logging
import random
import string
from io import StringIO
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='data_redaction.log'
)
logger = logging.getLogger(__name__)

def generate_random_ssn():
    """Generate a random SSN in format XXX-XX-XXXX"""
    return f"{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(1000, 9999)}"

def generate_random_string(length=10):
    """Generate a random string of specified length"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def redact_data(df, config):
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
                    logger.info(f"Redacted column '{column}' with random SSN")
                
                elif redact_type == 'random_string':
                    length = rules.get('length', 10)
                    df[column] = [generate_random_string(length) for _ in range(len(df))]
                    logger.info(f"Redacted column '{column}' with random string of length {length}")
                    
        return df
    
    except Exception as e:
        logger.error(f"Error in redacting data: {str(e)}")
        raise

def process_s3_file(bucket, input_key, config_path, output_bucket):
    """Main function to process S3 file and upload redacted version"""
    s3_client = boto3.client('s3')
    
    try:
        # Read configuration from YAML
        logger.info(f"Reading config file from {config_path}")
        config_obj = s3_client.get_object(Bucket=bucket, Key=config_path)
        config = yaml.safe_load(config_obj['Body'].read().decode('utf-8'))
        
        # Read CSV from S3
        logger.info(f"Reading input CSV from s3://{bucket}/{input_key}")
        obj = s3_client.get_object(Bucket=bucket, Key=input_key)
        df = pd.read_csv(obj['Body'])
        
        # Perform redaction
        redacted_df = redact_data(df, config)
        
        # Generate output filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_key = f"redacted/redacted_{timestamp}_{input_key.split('/')[-1]}"
        
        # Convert DataFrame to CSV string
        csv_buffer = StringIO()
        redacted_df.to_csv(csv_buffer, index=False)
        
        # Upload redacted file to S3
        logger.info(f"Uploading redacted file to s3://{output_bucket}/{output_key}")
        s3_client.put_object(
            Bucket=output_bucket,
            Key=output_key,
            Body=csv_buffer.getvalue()
        )
        
        logger.info("Data redaction completed successfully")
        return f"s3://{output_bucket}/{output_key}"
        
    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        raise

def main():
    """Entry point of the script"""
    # Example usage - modify these parameters as needed
    input_bucket = 'your-input-bucket'
    input_file_key = 'input/data.csv'
    config_file_key = 'config/redaction_config.yaml'
    output_bucket = 'your-output-bucket'
    
    try:
        result = process_s3_file(input_bucket, input_file_key, config_file_key, output_bucket)
        print(f"Redacted file uploaded to: {result}")
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
