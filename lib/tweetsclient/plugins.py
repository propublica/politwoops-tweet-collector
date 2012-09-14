#!/usr/bin/env python
# encoding: utf-8
"""
plugin.py

Created by Breyten Ernsting on 2010-08-06.
Copyright (c) 2010 Breyten Ernsting. All rights reserved.
"""

import sys
import os

import tweetsclient

class Plugin:
    def __init__(self, options = {}):
        self.options = options
        # get config
        self.config = tweetsclient.Config().get()
        # set verbose for debug output
        self.verbose = ('verbose' in options) and (options['verbose'])
    
class TrackPlugin(Plugin):
    def get_type(self):
        return 'stream' # or 'users' or 'words'
    
    def get_items(self):
        return []

class QueuePlugin(Plugin):
    def add(self, tweet):
        pass
    
    def connect(self):
        pass
    
    def disconnect(self):
        pass
