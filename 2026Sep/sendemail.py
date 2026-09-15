import sys
import smtplib
import mimetypes
from pathlib import Path
from email.message import EmailMessage

SMTP_HOST = "smtp.office365.com"
SMTP_PORT = 587

SMTP_USER = "noreply@vinci-construction.com"
SMTP_PASSWORD = "YOUR_PASSWORD"

TO_EMAIL = "recipient@company.com"

if len(sys.argv) < 2:
    print("Usage: python3 send_email.py <attachment_file>")
    sys.exit(1)

attachment_path = Path(sys.argv[1])

if not attachment_path.exists():
    print(f"Attachment not found: {attachment_path}")
    sys.exit(1)

msg = EmailMessage()

msg["Subject"] = "File Attachment"
msg["From"] = SMTP_USER
msg["To"] = TO_EMAIL

msg.set_content(
    f"""Hi,

Please find the attached file.

File: {attachment_path.name}

Regards,
Informatica
"""
)

content_type, encoding = mimetypes.guess_type(attachment_path)

if content_type is None:
    content_type = "application/octet-stream"

maintype, subtype = content_type.split("/", 1)

with open(attachment_path, "rb") as f:
    msg.add_attachment(
        f.read(),
        maintype=maintype,
        subtype=subtype,
        filename=attachment_path.name
    )

try:
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()

        smtp.login(SMTP_USER, SMTP_PASSWORD)

        smtp.send_message(msg)

    print("Email sent successfully")
    sys.exit(0)

except Exception as e:
    print(f"Failed to send email: {e}")
    sys.exit(1)
