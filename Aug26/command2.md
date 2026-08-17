# Informatica OS Command Email Notification System

This document contains the universal wrapper script and all command variations for production, testing on the client Informatica server, and local Mac testing.

---

## 1. Universal Wrapper Script (`new_os_command.sh` / `invoke_os_command_new.sh`)

Save this file on your Linux server (e.g., `/u02/infa_shared/Scripts/new_os_command.sh` or in your working directory) and grant execution permissions:
```bash
chmod +x new_os_command.sh
```

### Script Content:
```bash
#!/bin/bash
if [ $# -eq 0 ]; then
    echo "ERROR: No command argument provided" >&2
    exit 1
fi
/bin/bash -c "$*"
```

---

## 2. Production Command (Informatica / Client Server with `mailx`)

Use this command in your Informatica Command Task or run it directly on the Linux server. It uses the production `/u02/...` paths and triggers `mailx` to send the emails with attachments.

```bash
sh new_os_command.sh 'PARAM_FILE="/u02/infa_shared/ParamFiles/HUB_PIM/HUB_PIM.prm"; PROCESSED=" "; REGIONS=$(grep -i "^\$\$Region_email_fil=" "$PARAM_FILE" | cut -d"=" -f2 | tr -d "\"\r"); for REGION in $REGIONS; do for FILE in /u02/infa_shared/TgtFiles/PEP_NAV/$REGION/*_PEPcl_*.txt; do [ -e "$FILE" ] || continue; FNAME=$(basename "$FILE"); PREFIX="${FNAME%%_*}"; case " $PROCESSED " in *" $PREFIX "*) continue;; esac; PROCESSED="$PROCESSED$PREFIX "; EMAILS=$(grep -i "^\$\$${PREFIX}_email=" "$PARAM_FILE" | cut -d"=" -f2 | tr -d "\r "); if [ -n "$EMAILS" ]; then ATTACH_STR=""; FILE_LIST=""; for R in $REGIONS; do for F in /u02/infa_shared/TgtFiles/PEP_NAV/$R/${PREFIX}_PEPcl_*.txt; do [ -e "$F" ] || continue; FN=$(basename "$F"); ATTACH_STR="$ATTACH_STR -a $F"; FILE_LIST="${FILE_LIST}${FN}\n"; done; done; [ -n "$ATTACH_STR" ] && printf "Attached the NAVICL file(s) for %s:\n\n%b" "$PREFIX" "$FILE_LIST" | mailx -s "NAVICL File for $PREFIX" $ATTACH_STR $EMAILS; else echo "No email configuration found for prefix: $PREFIX"; fi; done; done'
```

---

## 3. Dry-Run / Test Command on Client Server (with `echo` instead of `mailx`)

Use this on the client Linux server to test the file scanning, region matching, and email recipient lookups without actually sending emails.

```bash
sh new_os_command.sh 'PARAM_FILE="/u02/infa_shared/ParamFiles/HUB_PIM/HUB_PIM.prm"; PROCESSED=" "; REGIONS=$(grep -i "^\$\$Region_email_fil=" "$PARAM_FILE" | cut -d"=" -f2 | tr -d "\"\r"); for REGION in $REGIONS; do for FILE in /u02/infa_shared/TgtFiles/PEP_NAV/$REGION/*_PEPcl_*.txt; do [ -e "$FILE" ] || continue; FNAME=$(basename "$FILE"); PREFIX="${FNAME%%_*}"; case " $PROCESSED " in *" $PREFIX "*) continue;; esac; PROCESSED="$PROCESSED$PREFIX "; EMAILS=$(grep -i "^\$\$${PREFIX}_email=" "$PARAM_FILE" | cut -d"=" -f2 | tr -d "\r "); if [ -n "$EMAILS" ]; then ATTACH_STR=""; FILE_LIST=""; for R in $REGIONS; do for F in /u02/infa_shared/TgtFiles/PEP_NAV/$R/${PREFIX}_PEPcl_*.txt; do [ -e "$F" ] || continue; FN=$(basename "$F"); ATTACH_STR="$ATTACH_STR -a $F"; FILE_LIST="${FILE_LIST}${FN}\n"; done; done; [ -n "$ATTACH_STR" ] && echo "[DRY-RUN DISPATCH] Prefix: $PREFIX | To: $EMAILS | Files: $ATTACH_STR"; else echo "No email configuration found for prefix: $PREFIX"; fi; done; done'
```

---

## 4. Local Mac Test Command (Local `simulation_new` Paths)

To test locally inside your Mac terminal inside the `simulation_new` directory:

```bash
sh new_os_command.sh 'PARAM_FILE="/Users/sathishkumardm/Pikachooz2.0/notsoimp/simulation_new/HUB_PIM.prm"; PROCESSED=" "; REGIONS=$(grep -i "^\$\$Region_email_fil=" "$PARAM_FILE" | cut -d"=" -f2 | tr -d "\"\r"); for REGION in $REGIONS; do for FILE in /Users/sathishkumardm/Pikachooz2.0/notsoimp/simulation_new/TgtFiles/PEP_NAV/$REGION/*_PEPcl_*.txt; do [ -e "$FILE" ] || continue; FNAME=$(basename "$FILE"); PREFIX="${FNAME%%_*}"; case " $PROCESSED " in *" $PREFIX "*) continue;; esac; PROCESSED="$PROCESSED$PREFIX "; EMAILS=$(grep -i "^\$\$${PREFIX}_email=" "$PARAM_FILE" | cut -d"=" -f2 | tr -d "\r "); if [ -n "$EMAILS" ]; then ATTACH_STR=""; FILE_LIST=""; for R in $REGIONS; do for F in /Users/sathishkumardm/Pikachooz2.0/notsoimp/simulation_new/TgtFiles/PEP_NAV/$R/${PREFIX}_PEPcl_*.txt; do [ -e "$F" ] || continue; FN=$(basename "$F"); ATTACH_STR="$ATTACH_STR -a $F"; FILE_LIST="${FILE_LIST}${FN}\n"; done; done; [ -n "$ATTACH_STR" ] && echo "[DISPATCH] Prefix: $PREFIX | To: $EMAILS | Files: $ATTACH_STR"; else echo "No email configuration found for prefix: $PREFIX"; fi; done; done'
```
