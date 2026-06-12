#!/bin/bash

# Script 1: Read through a source directory, find CSV files, create a list file, and generate an IICS parameter file.
# Usage: ./create_list.sh <task_id> [--filename-only | --full-path]

# Check if correct number of arguments are passed
if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
    echo "Usage: $0 <task_id> [--filename-only | --full-path]"
    exit 1
fi

TASK_ID="$1"
# PATH_FLAG="${2:---full-path}"
PATH_FLAG="${2:---filename-only}"

# Validate the flag
if [ "$PATH_FLAG" != "--full-path" ] && [ "$PATH_FLAG" != "--filename-only" ]; then
    echo "Error: Invalid flag '$PATH_FLAG'. Use '--full-path' or '--filename-only'."
    exit 1
fi

# Hardcoded paths
SRC_DIR="/Users/sathishkumar/Documents/MACMIX/Miami_work/Jun_2026/list_archive/src"
LST_FILE="/Users/sathishkumar/Documents/MACMIX/Miami_work/Jun_2026/list_archive/files.lst"
PARAM_FILE="/Users/sathishkumar/Documents/MACMIX/Miami_work/Jun_2026/list_archive/${TASK_ID}.param"

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

# Read files from LST_FILE and join them with commas
files=""
while IFS= read -r file_path || [ -n "$file_path" ]; do
    [ -z "$file_path" ] && continue

    if [ "$PATH_FLAG" = "--filename-only" ]; then
        item=$(basename "$file_path")
    else
        item="$file_path"
    fi

    if [ -z "$files" ]; then
        files="$item"
    else
        files="$files,$item"
    fi
done < "$LST_FILE"

# Create the IICS parameter file
# Standard Informatica format:
# #task_id
# $task_id=task_id
# $files_to_delete=files
cat << EOF > "$PARAM_FILE"
#${TASK_ID}
\$task_id=${TASK_ID}
\$files_to_delete=${files}
EOF

# Verify output and print completion message
if [ -s "$LST_FILE" ]; then
    echo "Success: CSV files found and listed in '$LST_FILE'"
    echo "IICS parameter file created at '$PARAM_FILE' (Format: $PATH_FLAG)"
else
    echo "Warning: No CSV files (*.csv, *.CSV) found in '$SRC_DIR'. List file is empty."
    echo "IICS parameter file created with empty files list at '$PARAM_FILE'"
fi


