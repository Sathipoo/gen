I checked the Linux/Informatica server for the available email-sending setup.
Currently, the server does not have the standard mail utilities installed, such as mail, mailx, sendmail, postfix, or msmtp. I also checked the standard SMTP ports (25, 465, and 587), and there is no local SMTP service running on the server.
Python 3 is available, so we can use Python to send emails if an SMTP relay/server is provided. However, Python still requires an SMTP endpoint to deliver the emails.
Next actions would be:
Confirm whether there is an existing corporate SMTP relay that this server is allowed to use.
Provide the SMTP hostname, port, authentication requirements, and permitted sender address, if available.
If no SMTP relay is currently available, configure/provide one for this server.
Optionally install s-nail/mailx if shell-based email sending is preferred. Otherwise, Python 3 can be used directly once SMTP connectivity is available.
Once the SMTP details are available, we can test connectivity from the server and validate email delivery.



Hi Team,
Could you please provide the SMTP details required for sending emails from the Informatica Linux server?
Specifically, please confirm:
SMTP server/relay hostname
Port
Whether TLS/STARTTLS is required
Whether authentication is required
If authentication is required, the approved authentication method and service account details
If authentication is not required, whether this server will be allow-listed/trusted based on IP/hostname
Permitted sender/From email address
Any restrictions on recipient domains or relay usage
Confirmation that outbound connectivity from the Informatica server to the SMTP endpoint is enabled
If possible, we would prefer an IP/hostname-based trusted relay so that credentials do not need to be stored in scripts.
