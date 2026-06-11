#!/bin/bash

# Script 2: Audit files listing their processed date, filename, and row count excluding the header.
# Usage: ./audit_files.sh <lst_file> <audit_csv_file>

# Check if correct number of arguments are passed
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <lst_file> <audit_csv_file>"
    exit 1
fi

LST_FILE="$1"
AUDIT_FILE="$2"

# Verify that the list file exists
if [ ! -f "$LST_FILE" ]; then
    echo "Error: List file '$LST_FILE' does not exist."
    exit 1
fi

# Ensure audit directory exists
mkdir -p "$(dirname "$AUDIT_FILE")" 2>/dev/null

# Initialize audit report CSV header if the file is empty or does not exist
if [ ! -s "$AUDIT_FILE" ]; then
    echo "Processed_Date,File_Name,Row_Count" > "$AUDIT_FILE"
fi

# Read list file line by line and audit each CSV
while IFS= read -r file_path || [ -n "$file_path" ]; do
    # Skip empty lines
    [ -z "$file_path" ] && continue

    # Verify that the CSV file exists
    if [ ! -f "$file_path" ]; then
        echo "Warning: File '$file_path' not found. Skipping."
        continue
    fi

    filename=$(basename "$file_path")
    processed_date=$(date "+%Y-%m-%d %H:%M:%S")

    # Get line count. If size is 0, row count is 0.
    if [ ! -s "$file_path" ]; then
        row_count=0
    else
        # Count number of lines
        line_count=$(wc -l < "$file_path" | tr -d ' ')
        
        # Exclude the header line (subtract 1)
        if [ "$line_count" -gt 0 ]; then
            row_count=$((line_count - 1))
        else
            row_count=0
        fi
    fi

    # Append audit details to the audit file
    echo "$processed_date,$filename,$row_count" >> "$AUDIT_FILE"

done < "$LST_FILE"

