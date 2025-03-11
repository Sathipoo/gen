#!/bin/bash
# Default parameter file location if not specified
PARAM_FILE=${1:-"./config.params"}

# Function to log messages
log() {
    local level=$1
    local message=$2
    local timestamp=$(date "+%Y-%m-%d %H:%M:%S")
    echo "[$timestamp] [$level] $message" | tee -a "$LOG_FILE"
}

# Function to validate multiple files with specific suffix
validate_files_with_suffix() {
    local suffix=$1
    local missing_count=0
    local file_list=""
    
    log "INFO" "Validating files with suffix: $suffix"
    
    # Loop through all required file patterns for this suffix
    for pattern in "${FILE_PATTERNS[@]}"; do
        local search_pattern="${pattern}*${suffix}"
        log "INFO" "Checking for file pattern: $search_pattern"
        
        # Search for the file in S3
        if aws s3 ls "$S3_BUCKET_PATH" | grep -q "$search_pattern"; then
            log "INFO" "Found file matching pattern: $search_pattern"
        else
            log "ERROR" "Missing file matching pattern: $search_pattern"
            missing_count=$((missing_count + 1))
            file_list="$file_list $search_pattern"
        fi
    done
    
    # Check for special file for 002 suffix
    if [ "$suffix" == "002.txt" ]; then
        local special_pattern="depbalbalcalc.summ*"
        log "INFO" "Checking for special file pattern: $special_pattern (exclusive to 002)"
        
        if aws s3 ls "$S3_BUCKET_PATH" | grep -q "$special_pattern"; then
            log "INFO" "Found special file matching pattern: $special_pattern"
        else
            log "ERROR" "Missing special file matching pattern: $special_pattern"
            missing_count=$((missing_count + 1))
            file_list="$file_list $special_pattern"
        fi
    fi
    
    # Return results
    if [ $missing_count -gt 0 ]; then
        log "ERROR" "Total $missing_count files missing with suffix $suffix: $file_list"
        return 1
    else
        log "INFO" "All required files with suffix $suffix are present"
        return 0
    fi
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
    LOG_FILE="./unzipped_validator.log"
    echo "LOG_FILE not defined in parameter file, using default: $LOG_FILE"
fi

# Initialize log file
log "INFO" "=== S3 Unzipped Files Validation Started ==="
log "INFO" "Using S3 Bucket Path: $S3_BUCKET_PATH"

# Define common file patterns to check for each suffix
FILE_PATTERNS=(
    "alssec"
    "gldetl"
    "ip_accret"
    "ip_accrual"
    "ip_amort"
    "ip_holdings"
    "ip_mgbanalys"
    "ip_mktloss1"
    "ip_pledged"
    "ip_scheddt1"
    "ip_schedules"
    "ip_secinvtry"
    "ip_skholding"
    "pmfail"
    "poacct"
    "PortfolioHoldingsG"
    "posinc"
    "regFr2502"
    "stsideplg"
)

# Define suffixes to check (corresponding to original zip files)
SUFFIXES=(
    "001.txt"
    "002.txt"
    "062.txt"
)

# Special case: Check for PortfolioHoldingsG*001.csv (not .txt)
# Adjust the pattern check for this specific file
csv_pattern="PortfolioHoldingsG"
for suffix in "${SUFFIXES[@]}"; do
    # Extract the numeric part from the suffix
    numeric_part=$(echo "$suffix" | cut -d'.' -f1)
    csv_search_pattern="${csv_pattern}*${numeric_part}.csv"
    
    log "INFO" "Checking for special CSV file pattern: $csv_search_pattern"
    
    if aws s3 ls "$S3_BUCKET_PATH" | grep -q "$csv_search_pattern"; then
        log "INFO" "Found special CSV file matching pattern: $csv_search_pattern"
    else
        log "ERROR" "Missing special CSV file matching pattern: $csv_search_pattern"
    fi
done

# Track overall validation status
validation_failed=false

# Check files for each suffix
for suffix in "${SUFFIXES[@]}"; do
    if ! validate_files_with_suffix "$suffix"; then
        validation_failed=true
    fi
done

# Final status and exit
if [ "$validation_failed" = true ]; then
    log "ERROR" "=== S3 Unzipped Files Validation Failed ==="
    log "ERROR" "One or more required files are missing. Check logs for details."
    exit 1
else
    log "INFO" "=== S3 Unzipped Files Validation Completed Successfully ==="
    log "INFO" "All required files for all suffixes are present."
    exit 0
fi
