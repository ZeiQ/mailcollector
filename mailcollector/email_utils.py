#!/opt/local/bin/python

# import imaplib
# import sys, signal
# from avro import schema, datafile, io
# import os
import re
import email
from email.utils import parsedate
from email.utils import getaddresses
from email.header import decode_header
# import inspect, pprint
# import getopt
import time
import base64
import urllib
from lepl.apps.rfc3696 import Email


class EmailUtils(object):
    def __init__(self):
        """This class contains utilities for parsing and extracting structure from raw UTF-8 encoded emails"""
        self.is_email = Email()  # Generate a validator for email addresses

    @staticmethod
    def strip_brackets(message_id):
        return str(message_id).strip('<>')

    @staticmethod
    def parse_date(date_string):
        tuple_time = parsedate(date_string)
        iso_time = time.strftime("%Y-%m-%dT%H:%M:%S", tuple_time)
        return iso_time

    @staticmethod
    def get_charset(raw_email):
        if (type(raw_email)) is str:
            raw_email = email.message_from_string(raw_email)
        else:
            raw_email = raw_email
        charset = None
        for c in raw_email.get_charsets():
            if c is not None:
                charset = c
                break
        return charset

    # '1011 (X-GM-THRID 1292412648635976421 RFC822 {6499}' --> 1292412648635976421
    @staticmethod
    def get_thread_id(thread_string):
        p = re.compile('\d+ \(UID (.+) RFC822.*')
        m = p.match(thread_string)
        if m:
            return m.group(1)
        else:
            return None

    def parse_addrs(self, addr_string):
        if addr_string:
            addresses = getaddresses([addr_string])
            validated = []
            for address in addresses:
                address_pair = {'real_name': None, 'address': None}
                if address[0]:
                    address_pair['real_name'] = self.handle_header(address[0])
                if self.is_email(address[1]):
                    address_pair['address'] = address[1]
                if not address[0] and not self.is_email(address[1]):
                    pass
                else:
                    validated.append(address_pair)
            if len(validated) == 0:
                validated = None
            return validated

    @staticmethod
    def handle_header(raw_header):
        if raw_header is None or raw_header == '':
            return ''
        else:
            dhs = decode_header(raw_header)
            decoded_header = ''
            for t in dhs:
                if t[1] is None:
                    charset = 'us-ascii'
                else:
                    charset = t[1]
                decoded_header = decoded_header + t[0].decode(charset)
            return decoded_header

    def process_email(self, raw_email, thread_id):
        msg = email.message_from_string(raw_email)
        subject = msg['Subject']
        # print 'SUBJECT: ', subject
        body = self.get_body(msg)

        # Without handling charsets, corrupt avros will get written
        charsets = msg.get_charsets()
        charset = None
        for c in charsets:
            if c is not None:
                charset = c
                break
        # print charset
        try:
            if charset:
                # subject = self.handle_subject(subject, charset)
                subject = self.handle_header(subject)
                body = body.decode(charset)
            else:
                charset = 'us-ascii'
                # subject = subject.decode(charset)
                subject = self.handle_header(subject)
                body = body.decode(charset)
                # return {}, charset
            print 'SUBJECT: ', subject
        except:
            return {}, charset
        try:
            from_value = self.parse_addrs(msg['From'])[0]
            print 'FROM: ', from_value['real_name'], from_value['address']
        except:
            return {}, charset

        avro_parts = dict({
            'message_id': self.strip_brackets(msg['Message-ID']),
            'thread_id': thread_id,
            'in_reply_to': self.strip_brackets(msg['In-Reply-To']),
            'subject': subject,
            'date': self.parse_date(msg['Date']),
            'body': body,
            'from': from_value,
            'tos': self.parse_addrs(msg['To']),
            'ccs': self.parse_addrs(msg['Cc']),
            'bccs': self.parse_addrs(msg['Bcc']),
            'reply_tos': self.parse_addrs(msg['Reply-To'])
        })
        return avro_parts, charset

    @staticmethod
    def get_body(msg):
        body = ''
        if msg:
            for part in msg.walk():
                if part.get_content_type() == 'text/plain':
                    content_transfer_encoding = part['Content-Transfer-Encoding']
                    payload = part.get_payload()
                    if content_transfer_encoding == 'base64':
                        payload = base64.b64decode(payload)
                    elif content_transfer_encoding == 'quoted-printable':
                        payload = urllib.unquote(payload.replace('=\r\n', '').replace('=', '%'))
                    else:
                        pass
                    body += payload
        return body
