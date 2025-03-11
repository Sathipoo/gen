#!/bin/bash

# Default parameter file location if not specified
PARAM_FILE=${1:-"./param_file.sh"}

# Function to log messages
log() {
    local level=$1
    local message=$2
    local timestamp=$(date "+%Y-%m-%d %H:%M:%S")
    echo "[$timestamp] [$level] $message" | tee -a "$LOG_FILE"
}

# Check if parameter file exists
if [ ! -f "$PARAM_FILE" ]; then
    echo "Parameter file not found: $PARAM_FILE"
    exit 1
fi

# Load parameters from file
source "$PARAM_FILE"

# Validate required parameters
if [ -z "$S3_BUCKET_PATH" ]; then
    echo "S3_BUCKET_PATH is not defined in parameter file"
    exit 1
fi

if [ -z "$LOG_FILE" ]; then
    LOG_FILE="./s3_validator.log"
    echo "LOG_FILE not defined in parameter file, using default: $LOG_FILE"
fi

# Initialize log file
log "INFO" "=== S3 File Validation Started ==="
log "INFO" "Using S3 Bucket Path: $S3_BUCKET_PATH"

# Define file prefixes to check
declare -a file_prefixes=(
    "001RegRpts_"
    "002RegRpts_"
    "062RegRpts_"
)

# Count for missing files
missing_files=0

# Check each file prefix
for prefix in "${file_prefixes[@]}"; do
    log "INFO" "Checking for file with prefix: $prefix"
    
    # Use aws s3 ls with wildcard pattern
    if aws s3 ls "$S3_BUCKET_PATH" | grep -q "${prefix}.*\.zip$"; then
        log "INFO" "File found with prefix: $prefix"
    else
        log "ERROR" "No file found with prefix: $prefix"
        missing_files=$((missing_files + 1))
    fi
done

# Check if any files were missing
if [ $missing_files -gt 0 ]; then
    log "ERROR" "$missing_files required file(s) are missing"
    log "ERROR" "=== S3 File Validation Failed ==="
    exit 1
else
    log "INFO" "All required files are present"
    log "INFO" "=== S3 File Validation Completed Successfully ==="
    exit 0
fi
