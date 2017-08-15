#!/usr/bin/env python
# encoding: utf-8
"""
beanstalk.py

Created by Breyten Ernsting on 2010-08-08.
Copyright (c) 2010 __MyCompanyName__. All rights reserved.
"""

import sys
import os
import unittest

import anyjson
import beanstalkc
import logbook

import tweetsclient

log = logbook.Logger(__name__)

class BeanstalkPlugin(tweetsclient.QueuePlugin):
    def __init__(self, options = {}):
        tweetsclient.QueuePlugin.__init__(self, options)
        self.beanstalk = None
        self.tube = options['tube']

    def _connect(self, host='localhost', port=11300, tube='politwoops'):
        beanstalk = beanstalkc.Connection(host=host, port=port)
        beanstalk.use(tube)
        beanstalk.watch(tube)
        return beanstalk

    def connect(self):
        log.debug("Initiating beanstalk connection for tube {tube}.",
                  tube=self.tube)
        self.beanstalk = self._connect(
            host=self.config.get('beanstalk', 'host'),
            port=int(self.config.get('beanstalk', 'port')),
            tube=self.tube,
        )

    def disconnect(self):
        self.beanstalk.close()

    def add(self, tweet):
        json_obj = anyjson.serialize(tweet)
        result = self.beanstalk.put(json_obj)
        log.debug(result)
        log.debug(json_obj.encode('utf-8'))
