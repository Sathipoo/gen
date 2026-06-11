#!/bin/bash

# Script 3: Archive the processed CSV files into an archive directory.
# Usage: ./archive_files.sh <lst_file> <archive_directory> <log_file>

# Check if correct number of arguments are passed
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <lst_file> <archive_directory> <log_file>"
    exit 1
fi

LST_FILE="$1"
ARCH_DIR="$2"
LOG_FILE="$3"

# Logger helper function
log_message() {
    local level="$1"
    local message="$2"
    local timestamp
    timestamp=$(date "+%Y-%m-%d %H:%M:%S")
    echo "[$timestamp] [$level] $message" | tee -a "$LOG_FILE"
}

# Verify that the list file exists
if [ ! -f "$LST_FILE" ]; then
    echo "Error: List file '$LST_FILE' does not exist."
    exit 1
fi

# Ensure log and archive directories exist
mkdir -p "$(dirname "$LOG_FILE")" 2>/dev/null
if [ ! -d "$ARCH_DIR" ]; then
    mkdir -p "$ARCH_DIR"
    log_message "INFO" "Created archive directory at '$ARCH_DIR'."
fi

log_message "INFO" "Starting archival process using list file '$LST_FILE'."

# Read list file line by line and move each CSV file
while IFS= read -r file_path || [ -n "$file_path" ]; do
    # Skip empty lines
    [ -z "$file_path" ] && continue

    # Verify that the CSV file exists before moving
    if [ ! -f "$file_path" ]; then
        log_message "WARNING" "File '$file_path' not found. Cannot archive. Skipping."
        continue
    fi

    filename=$(basename "$file_path")

    # Move the file to the archive directory
    if mv "$file_path" "$ARCH_DIR/"; then
        log_message "INFO" "Successfully archived '$filename' to '$ARCH_DIR/'"
    else
        log_message "ERROR" "Failed to archive '$filename' to '$ARCH_DIR/'"
    fi

done < "$LST_FILE"

log_message "INFO" "Archival process completed."
