#!/bin/bash

# Check if all arguments are provided
if [ "$#" -ne 4 ]; then
    echo "Usage: $0 <source_path> <config_file> <valid_list_file> <invalid_list_file>"
    exit 1
fi

SOURCE_PATH="$1"
CONFIG_FILE="$2"
VALID_LIST_FILE="$3"
INVALID_LIST_FILE="$4"
EMAIL_RECIPIENT="admin@example.com" # Change this to the actual recipient

# Clear or create output files
> "$VALID_LIST_FILE"
> "$INVALID_LIST_FILE"

# Check if config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Config file not found at $CONFIG_FILE"
    exit 1
fi

# Read the expected header from the config file
# Assuming the config file contains the expected header on the first line
EXPECTED_HEADER=$(head -n 1 "$CONFIG_FILE" | tr -d '\r')

# Iterate through files in the source path
for file in "$SOURCE_PATH"/*; do
    # Skip if it's not a regular file
    [ -f "$file" ] || continue

    filename=$(basename "$file")
    is_valid=true
    reason=""

    # Check 1: File size is zero
    if [ ! -s "$file" ]; then
        is_valid=false
        reason="File size is zero"
    else
        # Check 2: Metadata validation (Header check)
        # Read the first line of the file
        file_header=$(head -n 1 "$file" | tr -d '\r')

        if [ "$file_header" != "$EXPECTED_HEADER" ]; then
            is_valid=false
            reason="Header mismatch or invalid delimiter"
        else
            # Check 3: Header only (No data)
            # Count lines properly even if the last line has no newline
            line_count=$(awk 'END {print NR}' "$file")
            if [ "$line_count" -le 1 ]; then
                is_valid=false
                reason="File contains header only"
            fi
        fi
    fi

    if [ "$is_valid" = true ]; then
        echo "$file" >> "$VALID_LIST_FILE"
    else
        echo "$file : $reason" >> "$INVALID_LIST_FILE"
    fi
done

# Send email if there are invalid files
if [ -s "$INVALID_LIST_FILE" ]; then
    echo "Sending email with list of invalid files..."
    mail -s "Invalid Files Detected" "$EMAIL_RECIPIENT" < "$INVALID_LIST_FILE"
else
    echo "No invalid files found."
fi
