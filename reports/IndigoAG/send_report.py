#!/Users/eric/Documents/TINYpulse/data_analytics/DA_env/bin/python
"""
Author: Eric Lee
"""
import datetime
import httplib2
import pickle
import os
from apiclient import discovery
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# DA_package imports
from DA_package import auth, send_email


SCOPES = "https://mail.google.com/"
dir_path = os.path.dirname(os.path.realpath(__file__))


def main(
    file_name,
    sender="ericlee@tinypulse.com",
    receiver="ericlee@tinypulse.com",
    message="",
    subject="",
    dir_path="~/airflow/files/"
):
    authInst = auth.auth(SCOPES)
    credentials = authInst.get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build("gmail", "v1", http=http)

    html = """
        <html>

        <head>
            <title></title>
        </head>

        <body aria-readonly="false"><span style="color:#696969"><span style="background-color:rgb(255, 255, 255); 
                font-family:pt sans,myriad pro,arial,sans-serif; 
                font-size:12.8px">{}
            <br />
            &nbsp;
            <hr />
            <blockquote>
                <p><span style="color:#696969"><em><span
                                style="background-color:rgb(255, 255, 255); font-family:pt sans,myriad pro,arial,sans-serif; font-size:12.8px">This
                                email and any files transmitted with it are confidential and intended solely for the use of the
                                individual or entity to whom they are addressed. If you have received this email in error please
                                notify the system manager. This message contains confidential information and is intended only
                                for the individual named. If you are not the named addressee you should not disseminate,
                                distribute or copy this e-mail. Please notify the sender immediately by e-mail if you have
                                received this e-mail by mistake and delete this e-mail from your system. If you are not the
                                intended recipient you are notified that disclosing, copying, distributing or taking any action
                                in reliance on the contents of this information is strictly prohibited.</span></em></span></p>
            </blockquote>
        </body>

        </html>

        """.format(
        message
    )
    sendInst = send_email.SendEmail(service)
    message = sendInst.create_message_with_attachment(
        sender,
        receiver,
        subject,
        html,
        dir_path,
        file_name,
        message_type="html",
    )
    sendInst.send_message("me", message)


if __name__ == "__main__":
    main()
