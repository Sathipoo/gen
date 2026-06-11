#!/bin/bash

# Script 1: Read through a source directory and find all CSV files, creating a list file.
# Usage: ./create_list.sh <src_directory> <output_lst_file>

# Check if correct number of arguments are passed
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <src_directory> <output_lst_file>"
    exit 1
fi

SRC_DIR="$1"
LST_FILE="$2"

# Verify that the source directory exists
if [ ! -d "$SRC_DIR" ]; then
    echo "Error: Source directory '$SRC_DIR' does not exist."
    exit 1
fi

# Clear/create the destination list file
> "$LST_FILE"

# Find all CSV files in the source directory (case-insensitive extension)
# -maxdepth 1 limits search to the top-level directory (does not recurse)
# -type f filters for regular files
find "$SRC_DIR" -maxdepth 1 -type f \( -name "*.csv" -o -name "*.CSV" \) > "$LST_FILE"

# Verify output and print completion message
if [ -s "$LST_FILE" ]; then
    echo "Success: CSV files found and listed in '$LST_FILE'"
else
    echo "Warning: No CSV files (*.csv, *.CSV) found in '$SRC_DIR'. List file '$LST_FILE' is empty."
fi
