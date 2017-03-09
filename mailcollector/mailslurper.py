#!/opt/local/bin/python
import imaplib
import os
# import signal
import sys
from avro import schema, datafile, io
from email_utils import EmailUtils


class MailSlurper(object):
    def __init__(self):
        """This class downloads all emails in folders from your 163mail inbox
        and writes them as raw UTF-8 text in simple Avro records for further processing."""
        self.utils = EmailUtils()
        self.username = None
        self.password = None
        self.imap = None
        self.schema = None
        self.avro_writer = None
        self.avro_writertmp = None
        self.imap_folder = None
        self.id_list = None
        self.folder_count = None
        # Only the email BODY which RFC822.SIZE are smaller than 3M are fetched
        # otherwise the email HEADER are fetched.
        self.threshold_size = 3*1024*1024

    @staticmethod
    def init_directory(directory):
        if os.path.exists(directory):
            print 'Warning: %(directory)s already exists:' % {"directory": directory}
        else:
            os.makedirs(directory)
        return directory

    def init_imap(self, username, password):
        self.username = username
        self.password = password
        try:
            self.imap.shutdown()
        except AttributeError, imaplib.IMAP4_SSL.error:
            pass
        try:
            self.imap = imaplib.IMAP4_SSL('imap.163.com', 993)
            self.imap.login(username, password)
            self.imap.is_readonly = True
        except imaplib.IMAP4_SSL.error:
            pass

    # part_id will be helpful one we're splitting files among multiple slurpers
    def init_avro(self, output_path, part_id, schema_path):
        output_dir = None
        output_dirtmp = None  # Handle Avro Write Error
        if type(output_path) is str:
            output_dir = self.init_directory(output_path)
            output_dirtmp = self.init_directory(output_path + 'tmp')  # Handle Avro Write Error
        out_filename = '%(output_dir)s/part-%(part_id)s.avro' % \
                       {"output_dir": output_dir, "part_id": str(part_id)}
        out_filenametmp = '%(output_dirtmp)s/part-%(part_id)s.avro' % \
                          {"output_dirtmp": output_dirtmp, "part_id": str(part_id)}  # Handle Avro Write ERROR
        self.schema = open(schema_path, 'r').read()
        email_schema = schema.parse(self.schema)
        rec_writer = io.DatumWriter(email_schema)
        self.avro_writer = datafile.DataFileWriter(open(out_filename, 'wb'), rec_writer, email_schema)
        # CREATE A TEMP AvroWriter that can be used to workaround the UnicodeDecodeError
        # when writing into AvroStorage
        self.avro_writertmp = datafile.DataFileWriter(open(out_filenametmp, 'wb'), rec_writer, email_schema)

    def init_folder(self, folder):
        self.imap_folder = folder
        status, count = self.imap.select(folder)
        print "Folder '" + str(folder) + "' SELECT status: " + status
        if status == 'OK':
            count = int(count[0])
            ids = range(1, count)
            ids.reverse()
            self.id_list = ids
            print "Folder '" + str(folder) + " has " + str(count) + "' emails...\n"
            self.folder_count = count
        return status, count

    def timeout_handler(self, signum, frame):
        raise self.TimeoutException

    def fetch_size(self, email_id):
        size = 0
        try:
            status, data = self.imap.fetch(email_id, '(RFC822.SIZE)')
        except imaplib.IMAP4_SSL.error or imaplib.IMAP4_SSL.abort:
            return size
        if status == 'OK' and data[0] is not None:
            size = self.utils.get_size(data[0])
        return size

    def fetch_email(self, email_id):
        # signal.signal(signal.SIGALRM, self.timeout_handler)
        # signal.alarm(3000)  # triger alarm in 30 seconds

        try:
            if self.fetch_size(email_id) < self.threshold_size:
                status, data = self.imap.fetch(email_id, '(UID RFC822)')
            else:
                status, data = self.imap.fetch(email_id, '(UID RFC822.HEADER)')
            # 163's UID will get the thread of the message
        except self.TimeoutException:
            return 'TIMEOUT', {}, None
        except imaplib.IMAP4_SSL.abort:
            return 'ABORT', {}, None

        if status != 'OK' or data[0] is None:
            return 'ERROR', {}, None
        else:
            raw_thread_id = data[0][0]
            raw_email = data[0][1]
        try:
            thread_id = self.utils.get_thread_id(raw_thread_id)
            if thread_id is None:
                return 'ERROR', {}, None
            else:
                avro_record, charset = self.utils.process_email(raw_email, thread_id)
        except UnicodeDecodeError:
            return 'UNICODE', {}, None
        # except:
            # return 'ERROR', {}, None
        # Without a charset we pass bad chars to avro, and it dies. See AVRO-565.
        if charset:
            return status, avro_record, charset
        else:
            return 'CHARSET', {}, charset

    def shutdown(self):
        self.avro_writer.close()
        self.avro_writertmp.close()  # Handle Avro write errors
        self.imap.close()
        self.imap.logout()

    def write(self, record):
        # self.avro_writer.append(record)
        # BEGIN - Handle errors when writing into Avro storage
        try:
            self.avro_writertmp.append(record)
            self.avro_writer.append(record)
        except UnicodeDecodeError:
            sys.stderr.write("ERROR IN Writing EMAIL to Avro for UnicodeDecode issue, SKIPPED ONE\n")
            pass
        # except:
            # pass
            # END - Handle errors when writing into Avro storage

    def flush(self):
        self.avro_writer.flush()
        self.avro_writertmp.flush()  # Handle Avro write errors
        print "Flushed avro writer..."

    def slurp(self):
        if self.imap and self.imap_folder:
            for email_id in self.id_list:
                (status, email_hash, charset) = self.fetch_email(email_id)
                if status == 'OK' and charset and 'thread_id' in email_hash and 'from' in email_hash:
                    print email_id, charset, email_hash['thread_id']
                    self.write(email_hash)
                    if (int(email_id) % 1000) == 0:
                        self.flush()
                elif status == 'ERROR' or status == 'PARSE' or status == 'UNICODE' \
                        or status == 'CHARSET' or status == 'FROM':
                    sys.stderr.write("Problem fetching email id " + str(email_id) + ": " + status + "\n")
                    continue
                elif status == 'ABORT' or status == 'TIMEOUT':
                    sys.stderr.write("resetting imap for " + status + "\n")
                    stat, c = self.reset()
                    sys.stderr.write("IMAP RESET: " + str(stat) + " " + str(c) + "\n")
                else:
                    sys.stderr.write("ERROR IN PARSING EMAIL, SKIPPED ONE\n")
                    continue

    def reset(self):
        self.init_imap(self.username, self.password)
        try:
            status, count = self.init_folder(self.imap_folder)
        except:
            self.reset()
            status = 'ERROR'
            count = 0
        return status, count

    class TimeoutException(Exception):
        """Indicates an operation timed out."""
        # sys.stderr.write("Timeout exception occurred!\n")
        pass
