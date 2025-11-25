#!/bin/bash

# Wrapper script to convert Excel files to CSV using a Python script

# Check for correct number of arguments
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <source_directory> <excel_pattern> <destination_directory>"
    exit 1
fi

SOURCE_DIR="$1"
PATTERN="$2"
DEST_DIR="$3"

# Check if source directory exists
if [ ! -d "$SOURCE_DIR" ]; then
    echo "Error: Source directory '$SOURCE_DIR' does not exist."
    exit 1
fi

# Check if destination directory exists
if [ ! -d "$DEST_DIR" ]; then
    echo "Destination directory '$DEST_DIR' does not exist."
    exit 1
fi

SCRIPT_DIR=""
PYTHON_SCRIPT_NAME="convert_xlsx_to_csv.py"

PYTHON_SCRIPT="$SCRIPT_DIR/$PYTHON_SCRIPT_NAME"

if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo "Error: Python conversion script not found at '$PYTHON_SCRIPT'"
    exit 1
fi

echo "Starting conversion of files matching '$PATTERN' from '$SOURCE_DIR' to '$DEST_DIR'..."

# Find matching files and process them
find "$SOURCE_DIR" -maxdepth 1 -type f -name "$PATTERN" -print0 | while IFS= read -r -d '' file; do
    # Get the base filename
    filename=$(basename "$file")
    
    # Replace extension with .csv
    # This removes the extension from the filename and appends .csv
    base="${filename%.*}"
    output_file="$DEST_DIR/$base.csv"
    
    echo "Processing: $filename -> $base.csv"
    
    # Call the Python script
    python3 "$PYTHON_SCRIPT" "$file" "$output_file"
    
    if [ $? -ne 0 ]; then
        echo "Failed to convert '$file'"
    fi
done

echo "Conversion process completed."
