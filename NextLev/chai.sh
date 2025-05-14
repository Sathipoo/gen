#!/usr/bin/env bash

timezone="UTC"

# YYYYMMDD in the desired TZ
date_form=$(TZ="$timezone" date '+%Y%m%d')

# Grab the hour (00–23) in that TZ, strip any leading zero
HOUR=$(TZ="$timezone" date +%H | sed 's/^0*//')
: "${HOUR:=0}"

# Determine the seq file based on hour
if   (( HOUR >=  7 && HOUR <  9 )); then file_seq="01.txt"
elif (( HOUR >=  9 && HOUR < 12 )); then file_seq="02.txt"
elif (( HOUR >= 12 && HOUR < 15 )); then file_seq="03.txt"
elif (( HOUR >= 15 && HOUR < 18 )); then file_seq="04.txt"
elif (( HOUR >= 18 && HOUR < 20 )); then file_seq="05.txt"
elif (( HOUR >= 20 && HOUR < 23 )); then file_seq="06.txt"
else                                     file_seq="00.txt"
fi

file_name_prefix="PPNX/PPNXO"
tgt_filename="${file_name_prefix}${date_form}${file_seq}"

echo "Target filename: $tgt_filename"

# --- Create IICS parameter file ---

p_SrcFileName="afsactivity_2.txt"
p_SrcTgtFile="afsactivity_2.txt"

# Parameter file path/name
param_file="iics_params.param"

# Write out the file – each line is $$ParamName=Value
cat <<EOF > "$param_file"
\$\$p_SrcFileName=${p_SrcFileName}
\$\$p_SrcTgtFile=${p_SrcTgtFile}
\$\$p_TgtFileName=${tgt_filename}
EOF

echo "IICS parameter file created: $param_file"
