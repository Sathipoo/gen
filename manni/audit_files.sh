#!/bin/bash

# Script 2: Audit files with size and record count

# Function to display usage
usage() {
    echo "Usage: $0 <source_directory> <file_pattern> <audit_file> [header_option]"
    echo "  header_option:"
    echo "    --no-header   : File has no header. Count all lines as records."
    echo "    (default)     : File has header. First line is excluded from record count."
    exit 1
}

# Check for minimum arguments
if [ "$#" -lt 3 ]; then
    usage
fi

SOURCE_DIR="$1"
PATTERN="$2"
AUDIT_FILE="$3"
HEADER_OPT="$4"

# Check if source directory exists
if [ ! -d "$SOURCE_DIR" ]; then
    echo "Error: Source directory '$SOURCE_DIR' does not exist."
    exit 1
fi

# Initialize audit file with a header row
echo "File Path,Size,Record Count" > "$AUDIT_FILE"

echo "Starting audit for files matching '$PATTERN' in '$SOURCE_DIR'..."

# Find files and process them
find "$SOURCE_DIR" -maxdepth 1 -type f -name "$PATTERN" -print0 | while IFS= read -r -d '' file; do
    size=$(du "$file" | awk '{print $1}')
    
    # Get line count
    lines=$(wc -l < "$file" | tr -d ' ')
    
    # Adjust for header (Default: assume header exists and skip it)
    if [[ "$HEADER_OPT" != "--no-header" ]]; then
        if [ "$lines" -gt 0 ]; then
            lines=$((lines - 1))
        fi
    fi
    
    # Append to audit file
    echo "$file,$size,$lines" >> "$AUDIT_FILE"
done

echo "Audit completed. Report saved to '$AUDIT_FILE'"
