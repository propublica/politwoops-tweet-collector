#!/usr/bin/env python
# encoding: utf-8
"""
tweets-client.py

Created by Breyten Ernsting on 2010-05-30.
Copyright (c) 2010 Back-End-Front Web Development. All rights reserved.
"""

import sys
import getopt
import re
import urllib
import urllib2
import ConfigParser

import beanstalkc

from time import sleep

import socket
# disable buffering
socket._fileobject.default_bufsize = 0

import httplib
httplib.HTTPConnection.debuglevel = 1


# this is for consuming the streaming API
import tweetstream

import anyjson

# external libs
sys.path.insert(0, './lib')

import tweetsclient

help_message = '''
Usage: tweets-client.py [-v]

Options:
  -v     Show verbose output
'''


class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

class TweetStreamClient:
    def __init__(self, verbose, output = None):
        self.verbose = verbose
        self.output = output
        self.get_config()
        self.user = self.config.get('tweets-client', 'username')
        self.passwd = self.config.get('tweets-client', 'password')

    def get_config_default(self, section, key, default = None):
        try:
            return self.config.get(section, key)
        except ConfigParser.NoOptionError:
            return default
        
    def load_plugin(self, plugin_module, plugin_class):
        pluginModule = __import__(plugin_module)
        components = plugin_module.split('.')
        for comp in components[1:]:
            pluginModule = getattr(pluginModule, comp)
        pluginClass = getattr(pluginModule, plugin_class)
        return pluginClass

    def get_queue(self):
        queue_module = self.get_config_default('tweets-client', 'queue-module', 'tweetsclient.beanstalk')
        queue_class = self.get_config_default('tweets-client', 'queue-class', 'BeanstalkPlugin')
        self._debug("Loading queue plugin : %s - %s" % (queue_module, queue_class))
        pluginClass = self.load_plugin(queue_module, queue_class)
        plugin = pluginClass({'verbose': self.verbose})
        plugin.connect()
        return plugin

    def get_config(self):
        self._debug("Reading config ...")
        self.config = tweetsclient.Config().get()
    
    def get_stream(self):
        queue_module = self.get_config_default('tweets-client', 'track-module', 'tweetsclient.config_track')
        queue_class = self.get_config_default('tweets-client', 'track-class', 'ConfigTrackPlugin')
        self._debug("Loading track plugin : %s - %s" % (queue_module, queue_class))
        pluginClass = self.load_plugin(queue_module, queue_class)
        self.track = pluginClass({'verbose': self.verbose})
        #self.track = tweetsclient.MySQLTrackPlugin({'verbose': self.verbose})
        # self.track = tweetsclient.ConfigTrackPlugin({'verbose': self.verbose})
        stream_type = self.track.get_type()
        self._debug("Initializing a %s stream of tweets" % (stream_type))
        track_items = self.track.get_items()
        self._debug(track_items)
        stream = None
        if stream_type == 'users':
            stream = tweetstream.FilterStream(self.user, self.passwd, track_items)
        elif stream_type == 'words':
            stream = tweetstream.TrackStream(self.user, self.passwd, track_items)
        else:
            stream = tweetstream.TweetStream(self.user, self.passwd)            
        return stream

    def _debug(self, msg):
        if self.verbose:
            print >>sys.stderr, msg
    
    def run(self):
        self.queue = self.get_queue()
        self._debug("Setting up stream ...")
        stream = self.get_stream()
        self._debug("Done setting up stream ...")
        for tweet in stream:
            self.handle_tweet(stream, tweet)
        self.queue.disconnect()
        return 0

    def run_with_restart(self):
        # keeps tabs on whether we should restart the connection to Twitter ourselves
        shouldRestart = True
        # keeps tabs on how many times we've unsuccesfully restarted -- more means longer waiting times
        self.restartCounter = 0
        
        while shouldRestart:
            shouldRestart = False
            try:
                self.run()
            except (TypeError, tweetstream.ConnectionError, urllib2.HTTPError) as e:
                shouldRestart = True
                sleep(self.restartCounter * 60 * 5) # wait 5 extra minutes for each retry
                self.restartCounter += 1
                self._debug("Connection error, restarting for the %s time...\n\t%s" % (self.restartCounter, e.message))
    
    def handle_tweet(self, stream, tweet):
        # reset the restart counter once a tweet has come in
        self.restartCounter = 0
        # add the tweet to the queue
        self.queue.add(tweet)

def main(argv=None):
    if argv is None:
        argv = sys.argv
    try:
        try:
            opts, args = getopt.getopt(argv[1:], "ho:vr", ["help", "output=", "raise"])
        except getopt.error, msg:
            raise Usage(msg)
    
        # option processing
        verbose = False
        output = None
        harden = True
        for option, value in opts:
            if option == "-v":
                verbose = True
            if option in ("-h", "--help"):
                raise Usage(help_message)
            if option in ("-o", "--output"):
                output = value
            if option in ("-r", "--raise"):
                harden = False
    except Usage, err:
        print >> sys.stderr, sys.argv[0].split("/")[-1] + ": " + str(err.msg)
        print >> sys.stderr, "\t for help use --help"
        return 2
    if verbose:
        print "Starting .."
    app = TweetStreamClient(verbose, output)
    
    if harden:
        return app.run_with_restart()
    else:
        return app.run()

if __name__ == "__main__":
    sys.exit(main())
