import sys
import os
import smtplib
import mimetypes
from email.message import EmailMessage


SMTP_HOST = "smtp.office365.com"
SMTP_PORT = 587

SMTP_USER = "noreply@vinci-construction.com"
SMTP_PASSWORD = "YOUR_PASSWORD"


if len(sys.argv) != 6:
    print(
        "Usage: python3 send_email_generic.py "
        "<from_email> <to_email> <subject> <message> <attachment>"
    )
    sys.exit(1)


from_email = sys.argv[1]
to_email = sys.argv[2]
subject = sys.argv[3]
message = sys.argv[4]
attachment_path = sys.argv[5]


if not os.path.isfile(attachment_path):
    print("Attachment not found: {}".format(attachment_path))
    sys.exit(1)


msg = EmailMessage()

msg["From"] = from_email
msg["To"] = to_email
msg["Subject"] = subject

msg.set_content(message)


content_type, encoding = mimetypes.guess_type(attachment_path)

if content_type is None:
    content_type = "application/octet-stream"

maintype, subtype = content_type.split("/", 1)


with open(attachment_path, "rb") as f:
    msg.add_attachment(
        f.read(),
        maintype=maintype,
        subtype=subtype,
        filename=os.path.basename(attachment_path)
    )


try:
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as smtp:

        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()

        smtp.login(
            SMTP_USER,
            SMTP_PASSWORD
        )

        smtp.send_message(msg)

    print("Email sent successfully")
    print("Attachment: {}".format(attachment_path))

    sys.exit(0)

except Exception as e:

    print("Failed to send email: {}".format(e))
    sys.exit(1)
