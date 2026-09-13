I checked the Linux/Informatica server for the available email-sending setup.
Currently, the server does not have the standard mail utilities installed, such as mail, mailx, sendmail, postfix, or msmtp. I also checked the standard SMTP ports (25, 465, and 587), and there is no local SMTP service running on the server.
Python 3 is available, so we can use Python to send emails if an SMTP relay/server is provided. However, Python still requires an SMTP endpoint to deliver the emails.
Next actions would be:
Confirm whether there is an existing corporate SMTP relay that this server is allowed to use.
Provide the SMTP hostname, port, authentication requirements, and permitted sender address, if available.
If no SMTP relay is currently available, configure/provide one for this server.
Optionally install s-nail/mailx if shell-based email sending is preferred. Otherwise, Python 3 can be used directly once SMTP connectivity is available.
Once the SMTP details are available, we can test connectivity from the server and validate email delivery.
