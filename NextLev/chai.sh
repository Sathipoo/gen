#!/usr/bin/env bash
# Usage: ./file_seq.sh "YYYY-MM-DD HH:MM:SS"
DT="$1"

# Extract hour (00–23); strip leading zero to simplify numeric comparisons
HOUR=$(date -d "$DT" +%H | sed 's/^0*//')
: "${HOUR:=0}"  # fallback to 0 if empty

# Decide which file to output based on hour
if   [[ $HOUR -ge 7  && $HOUR -lt 9  ]]; then echo "01.txt"
elif [[ $HOUR -ge 9  && $HOUR -lt 12 ]]; then echo "02.txt"
elif [[ $HOUR -ge 12 && $HOUR -lt 15 ]]; then echo "03.txt"
elif [[ $HOUR -ge 15 && $HOUR -lt 18 ]]; then echo "04.txt"
elif [[ $HOUR -ge 18 && $HOUR -lt 20 ]]; then echo "05.txt"
elif [[ $HOUR -ge 20 && $HOUR -lt 23 ]]; then echo "06.txt"
else                                        echo "00.txt"
fi
