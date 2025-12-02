#!/bin/bash

# Configuration
SOURCE_PATH="/Users/sathishkumar/Documents/MACMIX/Miami_work/Nov_2025/test_source"
PATTERN="*.csv"
CONFIG_FILE="/Users/sathishkumar/Documents/MACMIX/Miami_work/Nov_2025/test_config.txt"
LIST_FILE_NAME="/Users/sathishkumar/Documents/MACMIX/Miami_work/Nov_2025/valid_files_list.txt"
AUDIT_FILE_NAME="/Users/sathishkumar/Documents/MACMIX/Miami_work/Nov_2025/audit_report.csv"
REJECTION_LIST_FILE_NAME="/Users/sathishkumar/Documents/MACMIX/Miami_work/Nov_2025/rejection_list.txt"

# Initialize output files
> "$LIST_FILE_NAME"
> "$REJECTION_LIST_FILE_NAME"
echo "File Name,Record Count" > "$AUDIT_FILE_NAME"

# Check if config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Config file not found at $CONFIG_FILE"
    exit 1
fi

# Read expected header
EXPECTED_HEADER=$(head -n 1 "$CONFIG_FILE" | tr -d '\r')

echo "Processing files in $SOURCE_PATH matching $PATTERN..."

# Iterate through files matching the pattern
find "$SOURCE_PATH" -maxdepth 1 -type f -name "$PATTERN" -print0 | while IFS= read -r -d '' file; do
    filename=$(basename "$file")
    is_valid=true
    reason=""
    

    
    # Check 1: File size is zero
    if [ ! -s "$file" ]; then
        is_valid=false
        reason="File size is zero"
    else
        # Check 2: Metadata validation (Header check)
        file_header=$(head -n 1 "$file" | tr -d '\r')
        
        if [ "$file_header" != "$EXPECTED_HEADER" ]; then
            is_valid=false
            reason="Header mismatch or invalid delimiter"
        else
            # Check 3: Header only (No data)
            line_count=$(awk 'END {print NR}' "$file")
            if [ "$line_count" -le 1 ]; then
                is_valid=false
                reason="File contains header only"
            fi
        fi
    fi
    
    if [ "$is_valid" = true ]; then
        # Add to valid list
        echo "$file" >> "$LIST_FILE_NAME"
        
        # Add to audit file (File Name, Size, Record Count)
        # Record count = Total lines - 1 (Header)
        record_count=$((line_count - 1))
        echo "$filename,$record_count" >> "$AUDIT_FILE_NAME"
    else
        # Add to rejection list
        echo "$filename : $reason" >> "$REJECTION_LIST_FILE_NAME"
    fi
done

echo "Processing complete."
echo "Valid files list: $LIST_FILE_NAME"
echo "Audit report: $AUDIT_FILE_NAME"
echo "Rejections: $REJECTION_LIST_FILE_NAME"
