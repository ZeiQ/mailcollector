#!/usr/bin/env python

# This is a command line utility for slurping emails from 163 and storing them as avro documents.
# I uses the Mail163Slurper class, which in turn uses email utils.

import os
import sys
import getopt
# from lepl.apps.rfc3696 import Email

from mailslurper import MailSlurper


def usage(context):
    print """Usage: mailcollector.py -m <mode: interactive|automatic> -u <username@163.com>
    -p <password> -s <schema_path> -f <imap_folder> -o <output_path>"""


def does_exist(path_string, name):
    if os.path.exists(path_string):
        pass
    else:
        print "Error: " + name + ": " + path_string + " does not exist."
        sys.exit(2)


def main():
    try:
        opts,  args = getopt.getopt(sys.argv[1:], 'm:u:p:s:f:o:i:')
    except getopt.GetoptError, err:
        # print help information and exit:
        print "Error:" + str(err)  # will print something like "option -a not recognized"
        usage("getopt")
        sys.exit(2)

    mode = None
    username = None
    password = None
    schema_path = None  # '../avro/email.schema'
    imap_folder = None  # '[Gmail]/All Mail'
    output_path = None
    single_id = None
    arg_check = dict()

    for o, a in opts:
        if o == "-m":
            mode = a
            if mode in ('automatic', 'interactive'):
                pass
            else:
                usage('opts')
                sys.exit(2)
            arg_check[o] = 1
        elif o in "-u":
            username = a
            arg_check[o] = 1
        elif o in "-p":
            password = a
            arg_check[o] = 1
        elif o in "-s":
            schema_path = a
            does_exist(schema_path, "filename")
            arg_check[o] = 1
        elif o in "-f":
            imap_folder = a
            arg_check[o] = 1
        elif o in "-o":
            output_path = a
            arg_check[o] = 1
        elif o in "-i":
            single_id = a
            arg_check[o] = a
        else:
            assert False, "unhandled option"

    if len(arg_check.keys()) >= 6:
        pass
    else:
        usage('numargs')
        sys.exit(2)

    if len(arg_check.keys()) == len(sys.argv[1:])/2:
        pass
    else:
        usage('badargs')
        sys.exit(2)

    slurper = MailSlurper()
    slurper.init_avro(output_path, 1, schema_path)
    slurper.init_imap(username, password)
    status, count = slurper.init_folder(imap_folder)
    if status == 'OK':
        if mode == 'automatic':
            # Grab a single message if its ID is specified - useful for debug
            if single_id:
                s, a, c = slurper.fetch_email(single_id)
                if s == 'OK':
                    print s, c, a
                else:
                    print "Problem fetching email ID: " + single_id + " - " + s
            # Otherwise grab them all
            else:
                print "Connected to folder " + imap_folder + " and downloading " + str(count) + " emails...\n"
                slurper.slurp()
                slurper.shutdown()
    else:
        print "Problem initializing imap connection."

if __name__ == "__main__":
    main()
