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
# import urllib
import quopri
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

    # '1011 (X-GM-THRID 1292412648635976421 RFC822 {6499}' --> 1292412648635976421
    @staticmethod
    def get_thread_id(thread_string):
        p = re.compile('\d+ \(UID (.+) RFC822.*')
        m = p.match(thread_string)
        if m:
            return m.group(1)
        else:
            return None

    @staticmethod
    def get_size(size_string):
        p = re.compile('\d+ \(RFC822\.SIZE +(\d+)\)')
        m = p.match(size_string)
        if m:
            return int(m.group(1))
        else:
            return 0

    def parse_addrs(self, addr_string):
        if addr_string:
            addr_string = self.handle_header(addr_string)
            addresses = getaddresses([addr_string])
            validated = []
            for address in addresses:
                address_pair = {'real_name': None, 'address': None}
                if address[0]:
                    address_pair['real_name'] = address[0]
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
    def raw_header_prehandle(raw):
        raw = raw.strip().replace('\r\n', '').replace('\n', '')
        raws = raw.split("?==?")
        length = len(raws)
        new_raws = []
        if length == 1:
            new_raws.append(raws[0])
        elif length == 2:
            new_raws.append(raws[0] + '?=')
            new_raws.append("=?" + raws[1])
        else:
            new_raws.append(raws[0] + '?=')
            for i in range(1, length-1):
                new_raws.append("=?" + raws[i] + "?=")
            new_raws.append("=?" + raws[length-1])
        return new_raws

    def handle_header(self, raw_header):
        if raw_header is None or raw_header == '':
            return ''
        else:
            # there are some raw_header have multiple lines
            raw_header_fragments = self.raw_header_prehandle(raw_header)
            news = []
            for fragment in raw_header_fragments:
                for hd in decode_header(fragment):
                    news.append(hd)
            decoded_header = ''
            for t in news:
                if t[1] is None:
                    charset = 'us-ascii'
                else:
                    charset = t[1]
                try:
                    tmp_decoded_header = t[0].decode(charset)
                except UnicodeDecodeError:
                    tmp_decoded_header = t[0]
                decoded_header = decoded_header + tmp_decoded_header
            return decoded_header

    def process_email(self, raw_email, thread_id):
        msg = email.message_from_string(raw_email)
        subject = self.handle_header(msg['Subject'])
        print 'SUBJECT: ', subject
        charset, body = self.get_body(msg)
        from_values = self.parse_addrs(msg['From'])
        if from_values is not None:
            from_value = self.parse_addrs(msg['From'])[0]
            print 'FROM: ', from_value['real_name'], from_value['address']
        else:
            from_value = ""

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
        first_charset = None
        if msg:
            for part in msg.walk():
                if part.get_content_type() == 'text/plain' or part.get_content_type() == 'text/html':
                    content_transfer_encoding = part['Content-Transfer-Encoding']
                    charset = part.get_content_charset()
                    if content_transfer_encoding is not None and charset is not None:
                        if first_charset is None:
                            first_charset = charset
                        payload = part.get_payload()
                        if content_transfer_encoding == 'base64':
                            payload = base64.b64decode(payload).decode(charset)
                        elif content_transfer_encoding == 'quoted-printable':
                            payload = quopri.decodestring(payload).decode(charset)
                        body += payload
        if first_charset is None:
            first_charset = 'us-ascii'
        return first_charset, body
