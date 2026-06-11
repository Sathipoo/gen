#!/bin/bash

# Script 3: Archive the processed CSV files into an archive directory with timestamp.
# Usage: ./archive_files.sh <lst_file> <archive_directory>

# Check if correct number of arguments are passed
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <lst_file> <archive_directory>"
    exit 1
fi

LST_FILE="$1"
ARCH_DIR="$2"

# Verify that the list file exists
if [ ! -f "$LST_FILE" ]; then
    echo "Error: List file '$LST_FILE' does not exist."
    exit 1
fi

# Ensure archive directory exists
if [ ! -d "$ARCH_DIR" ]; then
    mkdir -p "$ARCH_DIR"
fi

# Read list file line by line and move each CSV file
while IFS= read -r file_path || [ -n "$file_path" ]; do
    # Skip empty lines
    [ -z "$file_path" ] && continue

    # Verify that the CSV file exists before moving
    if [ ! -f "$file_path" ]; then
        echo "Warning: File '$file_path' not found. Cannot archive. Skipping."
        continue
    fi

    filename=$(basename "$file_path")
    timestamp=$(date "+%Y%m%d%H%M%S")

    # Determine the new timestamped filename
    if [[ "$filename" == *.* ]]; then
        base="${filename%.*}"
        ext="${filename##*.}"
        new_filename="${base}_${timestamp}.${ext}"
    else
        new_filename="${filename}_${timestamp}"
    fi

    # Move the file to the archive directory with the new name
    mv "$file_path" "$ARCH_DIR/$new_filename"

done < "$LST_FILE"

