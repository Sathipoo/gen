# main.py
import boto3
import logging

from parameters import (
    ZIP_LOCATION,
    UNZIP_LOCATION,
    REQUIRED_FILES,
    EXPECTED_TXT_COUNT,
    FILE_ENCODING
)

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Initialize S3 client
s3_client = boto3.client('s3')

# Custom exception
class FileProcessingError(Exception):
    pass

def get_s3_bucket_key(s3_path):
    """Extract bucket and key from S3 path"""
    path = s3_path.replace("s3://", "")
    bucket, key = path.split("/", 1)
    return bucket, key

def validate_initial_files():
    """Validate presence of required zip files in S3"""
    logger.info("Starting validation of required files")
    bucket, prefix = get_s3_bucket_key(ZIP_LOCATION)
    
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if 'Contents' not in response:
            raise FileProcessingError("No files found in zip location")
            
        found_files = {obj['Key'].split('/')[-1] for obj in response['Contents']}
        missing_files = set(REQUIRED_FILES) - found_files
        
        if missing_files:
            error_msg = f"Missing required files: {', '.join(missing_files)}"
            logger.error(error_msg)
            raise FileProcessingError(error_msg)
            
        logger.info("All required files found successfully")
        
    except Exception as e:
        logger.error(f"AWS S3 error during validation: {str(e)}")
        raise FileProcessingError(f"S3 access error: {str(e)}")

def validate_txt_files_count():
    """Validate count of txt files in unzip location"""
    logger.info("Validating txt files count")
    bucket, prefix = get_s3_bucket_key(UNZIP_LOCATION)
    
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        txt_files = [obj['Key'] for obj in response.get('Contents', []) 
                    if obj['Key'].endswith('.txt')]
        txt_count = len(txt_files)
        
        logger.info(f"Found {txt_count} txt files")
        if txt_count != EXPECTED_TXT_COUNT:
            error_msg = f"Expected {EXPECTED_TXT_COUNT} txt files, but found {txt_count}"
            logger.error(error_msg)
            raise FileProcessingError(error_msg)
            
        return txt_files
        
    except Exception as e:
        logger.error(f"AWS S3 error during txt count validation: {str(e)}")
        raise FileProcessingError(f"S3 access error: {str(e)}")

def check_file_content(s3_file_key, no_data_files):
    """Check if file has second record"""
    bucket, _ = get_s3_bucket_key(UNZIP_LOCATION)
    
    try:
        logger.debug(f"Processing file: {s3_file_key}")
        obj = s3_client.get_object(Bucket=bucket, Key=s3_file_key)
        content = obj['Body'].read().decode(FILE_ENCODING)
        lines = content.splitlines()
        
        if len(lines) < 2:
            logger.warning(f"File {s3_file_key} has no data (less than 2 lines)")
            no_data_files.append(s3_file_key)
            return False
            
        logger.debug(f"File {s3_file_key} has valid data")
        return True
        
    except Exception as e:
        logger.error(f"Error reading file {s3_file_key}: {str(e)}")
        raise FileProcessingError(f"Error processing file {s3_file_key}: {str(e)}")

def main():
    no_data_files = []
    
    try:
        # Step 1: Validate initial zip files
        validate_initial_files()
        
        # Step 2: Validate txt files count
        txt_files = validate_txt_files_count()
        
        # Step 3: Check content of each file
        logger.info("Starting content validation for all txt files")
        for file_key in txt_files:
            check_file_content(file_key, no_data_files)
        
        # Step 4: Check if any files had no data
        if no_data_files:
            error_msg = f"Found {len(no_data_files)} files with no data: {', '.join(no_data_files)}"
            logger.error(error_msg)
            raise FileProcessingError(error_msg)
            
        logger.info("File processing completed successfully")
        
    except FileProcessingError as e:
        logger.error(f"Processing failed: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error occurred: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()