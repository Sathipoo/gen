#!/bin/bash

# Script 3: Archive CSV files from a source directory into an archive directory with timestamp.
# Usage: ./archive_files.sh <src_directory> <archive_directory>

# Check if correct number of arguments are passed
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <src_directory> <archive_directory>"
    exit 1
fi

SRC_DIR="$1"
ARCH_DIR="$2"

# Verify that the source directory exists
if [ ! -d "$SRC_DIR" ]; then
    echo "Error: Source directory '$SRC_DIR' does not exist."
    exit 1
fi

# Ensure archive directory exists
if [ ! -d "$ARCH_DIR" ]; then
    mkdir -p "$ARCH_DIR"
fi

# Find all CSV files in the source directory (case-insensitive extension) and move them
# -maxdepth 1 limits search to the top-level directory (does not recurse)
# -type f filters for regular files
find "$SRC_DIR" -maxdepth 1 -type f \( -name "*.csv" -o -name "*.CSV" \) -print0 | while IFS= read -r -d '' file_path; do
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
done


