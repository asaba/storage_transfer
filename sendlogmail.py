'''
Created on 06/gen/2016

@author: sabah
'''

import smtplib
# from os.path import basename
# from email.mime.application import MIMEApplication
# from email.mime.multipart import MIMEMultipart
# from email.mime.text import MIMEText
# from email.utils import COMMASPACE, formatdate

sender = 'storage@storage-200.srt.inaf.it'
receivers = ['asaba@oa-cagliari.inaf.it', "sabahome@gmail.com", "fara@oa-cagliari.inaf.it"]

header_message = """From: Storage Manager <storage@storage-200.srt.inaf.it>
To: Andrea Saba <asaba@oa-cagliari.inaf.it>
Subject: [storage-200.srt.inaf.it] rsync log

"""


def sendmail(message):
    try:
        smtp_obj = smtplib.SMTP('localhost', 25)
        smtp_obj.sendmail(sender, receivers, header_message + message)
    except smtplib.SMTPException:
        print("Error: unable to send email")
