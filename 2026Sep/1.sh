#!/bin/bash

RUN_ID="$1"

PROCESS_NAME="testProcess"
TARGET_PATH="/home/infa/Temp"

TO_EMAIL="recipient@company.com"
FROM_EMAIL="noreply@vinci-construction.com"
SUBJECT="Test Process Output - Run ID ${RUN_ID}"
MESSAGE="Hi,

Please find attached the output file for ${PROCESS_NAME}.

Run ID: ${RUN_ID}

Regards,
Informatica
"

PYTHON_SCRIPT="/home/infa/Temp/send_email_generic.py"

ATTACHMENT_FILE="${TARGET_PATH}/${PROCESS_NAME}_${RUN_ID}.csv"

if [ -z "$RUN_ID" ]; then
    echo "Usage: $0 <run_id>"
    exit 1
fi

if [ ! -f "$ATTACHMENT_FILE" ]; then
    echo "Attachment file not found: $ATTACHMENT_FILE"
    exit 1
fi

python3 "$PYTHON_SCRIPT" \
    "$FROM_EMAIL" \
    "$TO_EMAIL" \
    "$SUBJECT" \
    "$MESSAGE" \
    "$ATTACHMENT_FILE"

exit $?
