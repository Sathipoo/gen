#!/bin/bash

# Check if an argument is provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 <path/to/file>"
    exit 1
fi

# Extract path and filename from the argument
FILE_PATH="$1"
DIR_PATH=$(dirname "$FILE_PATH")
FILE_NAME=$(basename "$FILE_PATH")

# Function to check if file exists
check_file() {
    if [ -f "$FILE_PATH" ]; then
        echo "File $FILE_PATH found. Exiting cleanly."
        exit 0
    else
        echo "File $FILE_PATH not found."
        return 1
    fi
}

# Initial check
check_file

# Retry loop: check 3 times with 5-minute intervals
for ((i=1; i<=3; i++)); do
    echo "Attempt $i of 3: Waiting 5 minutes before next check..."
    sleep 5  # 5 minutes in seconds
    check_file
done

# If file is still not found, create it with a single empty space
echo "File $FILE_PATH not found after 3 attempts. Creating file..."
mkdir -p "$DIR_PATH"  # Create directory if it doesn't exist
echo " " > "$FILE_PATH"  # Create the file with a single empty space
echo "File $FILE_PATH created with a single empty space."
exit 0
