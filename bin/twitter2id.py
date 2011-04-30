#!/usr/bin/env python
# encoding: utf-8
"""
twitter2id.py

Created by Breyten Ernsting on 2010-05-31.
Copyright (c) 2010 __MyCompanyName__. All rights reserved.
"""

import sys
import getopt

import twython

help_message = '''
The help message goes here.
'''


class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg


def main(argv=None):
    if argv is None:
        argv = sys.argv
    try:
        try:
            opts, args = getopt.getopt(argv[1:], "ho:vn:u:p:", ["help", "output=", "names=", "user=", "password="])
        except getopt.error, msg:
            raise Usage(msg)
    
        # option processing
        for option, value in opts:
            if option == "-v":
                verbose = True
            if option in ("-h", "--help"):
                raise Usage(help_message)
            if option in ("-o", "--output"):
                output = value
            if option in ("-n", "--names"):
                names = value.split(',')
            if option in ("-u", "--user"):
                username = value
            if option in ("-p", "--password"):
                password = value
    
        twitter = twython.core.setup(username=username, password=password)
        infos = twitter.bulkUserLookup(screen_names=names)
        ids = [info['id'] for info in infos]
        print u','.join([str(id) for id in ids])
    except Usage, err:
        print >> sys.stderr, sys.argv[0].split("/")[-1] + ": " + str(err.msg)
        print >> sys.stderr, "\t for help use --help"
        return 2


if __name__ == "__main__":
    sys.exit(main())
