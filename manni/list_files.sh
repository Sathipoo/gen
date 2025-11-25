#!/bin/bash

# Script 1: List matching files to a destination file

# Check for correct number of arguments
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <source_directory> <file_pattern> <destination_file>"
    exit 1
fi

SOURCE_DIR="$1"
PATTERN="$2"
DEST_FILE="$3"

# Check if source directory exists
if [ ! -d "$SOURCE_DIR" ]; then
    echo "Error: Source directory '$SOURCE_DIR' does not exist."
    exit 1
fi

# Create/Clear the destination file
> "$DEST_FILE"

echo "Searching for files matching '$PATTERN' in '$SOURCE_DIR'..."

# Find files matching the pattern in the source directory
# -maxdepth 1 ensures we only look in the specified directory, not subdirectories
# We redirect the output to the destination file
find "$SOURCE_DIR" -maxdepth 1 -type f -name "$PATTERN" >> "$DEST_FILE"

# Check if any files were found
if [ -s "$DEST_FILE" ]; then
    echo "Success: Matching files listed in '$DEST_FILE'"
else
    echo "Warning: No files matching '$PATTERN' found in '$SOURCE_DIR'."
fi
