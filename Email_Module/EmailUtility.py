#!/usr/bin/python

"""
Created on            :Sep 06, 2019
Modified By           :
Date Of Modification  :
Purpose               :sending emails
"""

import os
import re
import sys
import logging
from logging.handlers import TimedRotatingFileHandler

from smtplib import SMTP_SSL              # secure SMTP protocol (port 465, uses SSL)
from smtplib import SMTP                  # standard SMTP protocol(port 25, no encryption)

import mimetypes
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from xml.dom.minidom import parse
from xml.parsers.expat import ExpatError


################### adding log module ###################

script_path = os.path.dirname(os.path.abspath(__file__))
corelog_path = os.path.join(script_path, '../Log_Module')

if not os.path.exists(corelog_path):
    print ''
    print 'ERROR :From pycore.EmailUtility :CORE LOG PATH NOT FOUND [%s]' % (corelog_path)
    print 'Exiting Form Program Execution.'
    print ''
    sys.exit()
else:
    sys.path.append(corelog_path)

try:
    from LogUtility import Logger
except ImportError, e:
    print ''
    print 'ERROR :Core Modules Import Error :' + str(e)
    print 'Exiting Form Program Execution.'
    print ''
    sys.exit()

############################################################

class EmailService(object):
    MAIL_REG_EXP_COMPILE = re.compile(r'^[a-zA-Z0-9._%-+]+@[a-zA-Z0-9._%-]+.[a-zA-Z]{2,6}$')

    def __init__(self):
        self.invalid_ids = None
        self.text_mime = None
        self.html_mime = None
        self.attach_mime = None
        self.recipients = None
        self.mail = None
        self.frm = None

    @staticmethod
    def get_error(errorlog=None):
        if errorlog:
            print ''
            print errorlog
            print 'Exiting Form Program Execution.'
            print ''
            sys.exit()

    @staticmethod
    def validate_file(cls, filename):
        if not os.path.isfile(filename):
            return None

        directory = os.path.dirname(filename)
        abs_path = filename

        if not directory.strip():
            directory = os.getcwd()
            abs_path = os.path.join(directory, filename)

        return abs_path

    def set_mailheader(self, args):
        subject = args.get('subject', None)
        to = args.get('to', None)
        self.frm = frm = args.get('from', None)
        cc = args.get('cc', None)
        bcc = args.get('bcc', None)

        if subject and to and frm:
            self._prepare_email(subject, to, cc, frm, bcc)
        else:
            raise FlyTxtEmailError("""Sorry, We can't prepare the mail header as it misses the important fields""")

    def set_high_priority(self):
        if self.mail:
            self.mail['X-Priority'] = '1'
            self.mail['X-MSMail-Priority'] = 'High'

    def prepare_text_body(self, text):
        self.text_mime = MIMEText(text, 'plain')
        if self.mail:
            self.mail.attach(self.text_mime)

    def prepare_html_body(self, html_template):
        self.html_mime = MIMEText(html_template, 'html')
        if self.mail:
            self.mail.attach(self.html_mime)

    def set_recipients(self, recipients):
        if len(recipients) == 0:
            raise FlyTxtEmailError("Sorry, Recipients list is empty.")

        self.recipients = recipients
        self.invalid_ids = self._validate_all_emailids()

    def _prepare_email(self, subject, to, cc, frm, bcc):
        mail = MIMEMultipart()
        mail['Subject'] = subject
        mail['To'] = ','.join(to)
        mail['From'] = frm
        mail['CC'] = ','.join(cc)
        mail['BCC'] = ','.join(bcc)
        mail.preamble = 'You will not see this in a MIME-aware mail reader.\n'

        self.mail = mail

    def send_ssl(self, host, port, username, password, priorty=None):
        success = False
        err = ''
        if not self.recipients:
            raise FlyTxtEmailError("Sorry, You dont set the recipients details")

        if not self.mail:
            raise FlyTxtEmailError("Sorry, You don't set the mail header")

        try:
            conn = SMTP_SSL(host, port)
            conn.set_debuglevel(False)
            conn.login(username, password)
            try:
                conn.sendmail(self.frm, self.recipients, self.mail.as_string())
            finally:
                conn.close()

            success = True
        except Exception, e:
            err = e

        return (success, err)

    def send_standard(self, host, port, username, password, priorty=None):
        success = False
        err = ''
        if not self.recipients:
            raise FlyTxtEmailError("Sorry, You dont set the recipients details")

        if not self.mail:
            raise FlyTxtEmailError("Sorry, You don't set the mail header")

        try:
            conn = SMTP(host, port)
            conn.set_debuglevel(False)
            conn.login(username, password)
            try:
                conn.sendmail(self.frm, self.recipients, self.mail.as_string())
            finally:
                conn.close()

            success = True
        except Exception, e:
            err = e

        return (success, err)

    def send(self, host, port, priorty=None):
        success = False
        err = ''
        if not self.recipients:
            raise FlyTxtEmailError("Sorry, You dont set the recipients details")

        if not self.mail:
            raise FlyTxtEmailError("Sorry, You don't set the mail header")

        try:
            conn = SMTP(host, port)
            conn.sendmail(self.frm, self.recipients, self.mail.as_string())
            conn.quit()
            success = True
        except Exception, e:
            err = e

        return (success, err)

    def hook_attachment(self, attachment_file, attach_name=None):
        ctype, encoding = mimetypes.guess_type(attachment_file)
        if not ctype:
            ctype = 'application/ms-excel'

        maintype, subtype = ctype.split('/', 1)

        fp = open(attachment_file, 'rb')
        msg = MIMEBase(maintype, subtype)
        msg.set_payload(fp.read())
        fp.close()

        encoders.encode_base64(msg)

        if not attach_name:
            attach_name = os.path.basename(attachment_file)

        msg.add_header('Content-Disposition', 'attachment', filename=attach_name)
        self.attach_mime = msg
        if self.mail:
            self.mail.attach(self.attach_mime)

    def get_invalid_emailids(self):
        return self.invalid_ids

    def _validate_all_emailids(self):
        invalid_ids = []
        for mail_id in self.recipients:
            if not self._validate_email(mail_id):
                invalid_ids.append(mail_id)

        return invalid_ids

    def _validate_email(self, email_id):
        valid = True
        if not self.MAIL_REG_EXP_COMPILE.match(email_id.strip()):
            valid = False

        return valid

    '''Returns the file name if it has a valid path'''
    def _check_file_validity(self, name):
        abs_path = EmailService.validate_file(name)
        if not abs_path:
            raise FlyTxtXMLError("File you wish to attach to the mail '%s' doesn't exist." % name)
        return abs_path

    def checkFile(self, log, chkfile):
        success = False
        if os.path.exists(chkfile):
            if os.path.isfile(chkfile):
                if  os.stat(chkfile)[6]==0:
                    log.setLog('DEBUG','Attachment file is empty [%s]' % (chkfile))
                else:
                    success = True
            else:
                if os.path.isdir(chkfile):
                    log.setLog('ERROR', 'This is a directory [%s]' % (chkfile))
                else:
                    log.setLog('ERROR', 'Error in attachment file path [%s]' % (chkfile))
        else:
            log.setLog('DEBUG','Attachment file does not exist [%s]' % (chkfile))

        return success

    def sendmail1(self, log, email_conf, recipients, smtp_ip, smtp_port, mailbody_html, email_attachment = None):
        log.setLog('INFO', 'I am in sendmail1')
        email = EmailService()
        email.set_mailheader(email_conf)
        email.set_recipients(recipients)
        email.prepare_html_body(mailbody_html)
        if email_attachment:
                    email.hook_attachment(email_conf[email_attachment])

        status, error = email.send(smtp_ip, smtp_port)
        if status:
            log.setLog('INFO', 'Mail has been send Successfully')
        else:
            log.setLog('ERROR', 'Mail send error: %s :SMTP Server is Not Responding' % (error[-1]))

    def sendmail(self, log, email_conf, mailbody_html, attachment):
        log.setLog('INFO', 'Extracting Email Config details')
        conf = email_conf["email"].copy()
        recipients = []
        recipients.extend(conf["to"])
        recipients.extend(conf["cc"])
        recipients.extend(conf["bcc"])
        recipients = filter(None, list(set(recipients)))

        smtp_ip = email_conf["smtp"]["server"]
        smtp_port = email_conf["smtp"]["port"]

        self.set_mailheader(conf)
        self.set_recipients(recipients)
        self.prepare_html_body(mailbody_html)
        if attachment:
            log.setLog('INFO', 'Attachment Required :True')

            attachment_list = conf["attachment"]
            if not attachment_list:
                log.setLog('ERROR', 'No attachment file is mentioned in config file')
            else:
                for attachment_file in attachment_list:
                    file_chk_status = self.checkFile(log, attachment_file)
                    if file_chk_status:
                        log.setLog('INFO', 'Attachment File :%s' % attachment_file)
                        self.hook_attachment(attachment_file)
        else:
            log.setLog('INFO', 'Attachment Required :False')

        status, error = self.send(smtp_ip, smtp_port)
        if status:
            log.setLog('INFO', 'Mail has been send Successfully')
        else:
            log.setLog('ERROR', 'Mail send error: %s :SMTP Server is Not Responding' % (error[-1]))


    def secure_sendmail(self, log, email_conf, mailbody_html, attachment):
        log.setLog('INFO', 'Extracting Email Config details')
        conf = email_conf["email"].copy()
        recipients = []
        recipients.extend(conf["to"])
        recipients.extend(conf["cc"])
        recipients.extend(conf["bcc"])
        recipients = filter(None, list(set(recipients)))

        smtp_ip = email_conf["smtp"]["server"]
        smtp_port = email_conf["smtp"]["port"]
        smtp_internet_service_provider_username = email_conf["smtp"]["username"]
        smtp_internet_service_provider_password = email_conf["smtp"]["password"]

        self.set_mailheader(conf)
        self.set_recipients(recipients)
        self.prepare_html_body(mailbody_html)
        if attachment:
            log.setLog('INFO', 'Attachment Required :True')

            attachment_list = conf["attachment"]
            if not attachment_list:
                log.setLog('ERROR', 'No attachment file is mentioned in config file')
            else:
                for attachment_file in attachment_list:
                    file_chk_status = self.checkFile(log, attachment_file)
                    if file_chk_status:
                        log.setLog('INFO', 'Attachment File :%s' % attachment_file)
                        self.hook_attachment(attachment_file)
        else:
            log.setLog('INFO', 'Attachment Required :False')

        status, error = self.send_standard(smtp_ip, smtp_port, smtp_internet_service_provider_username, smtp_internet_service_provider_password)
        if status:
            log.setLog('INFO', 'Mail has been send Successfully')
        else:
            log.setLog('ERROR', 'Mail send error: %s :SMTP Server is Not Responding' % (error[-1]))

    def secure_ssl_sendmail(self, log, email_conf, mailbody_html, attachment):
        log.setLog('INFO', 'Extracting Email Config details')
        conf = email_conf["email"].copy()
        recipients = []
        recipients.extend(conf["to"])
        recipients.extend(conf["cc"])
        recipients.extend(conf["bcc"])
        recipients = filter(None, list(set(recipients)))

        smtp_ip = email_conf["smtp"]["server"]
        smtp_port = email_conf["smtp"]["port"]
        smtp_internet_service_provider_username = email_conf["smtp"]["username"]
        smtp_internet_service_provider_password = email_conf["smtp"]["password"]

        self.set_mailheader(conf)
        self.set_recipients(recipients)
        self.prepare_html_body(mailbody_html)
        if attachment:
            log.setLog('INFO', 'Attachment Required :True')

            attachment_list = conf["attachment"]
            if not attachment_list:
                log.setLog('ERROR', 'No attachment file is mentioned in config file')
            else:
                for attachment_file in attachment_list:
                    file_chk_status = self.checkFile(log, attachment_file)
                    if file_chk_status:
                        log.setLog('INFO', 'Attachment File :%s' % attachment_file)
                        self.hook_attachment(attachment_file)
        else:
            log.setLog('INFO', 'Attachment Required :False')

        status, error = self.send_ssl(smtp_ip, smtp_port, smtp_internet_service_provider_username, smtp_internet_service_provider_password)
        if status:
            log.setLog('INFO', 'Mail has been send Successfully')
        else:
            log.setLog('ERROR', 'Mail send error: %s :SMTP Server is Not Responding' % (error[-1]))
#END
