#!/usr/bin/env python
# encoding: utf-8
"""
tweets-client.py

Created by Breyten Ernsting on 2010-05-30.
Copyright (c) 2010 Back-End-Front Web Development. All rights reserved.
"""

import os
import sys
import argparse
import signal
import ConfigParser

import socket
# disable buffering
socket._fileobject.default_bufsize = 0

import httplib
httplib.HTTPConnection.debuglevel = 1

import anyjson
import logbook

# this is for consuming the streaming API
import tweetstream
import tweetsclient
import politwoops


_script_ = (os.path.basename(__file__)
            if __name__ == "__main__"
            else __name__)
log = logbook.Logger(_script_)

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

class TweetStreamClient:
    def __init__(self):
        self.config = tweetsclient.Config().get()
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

    def init_beanstalk(self):
        tweets_tube = self.config.get('beanstalk', 'tweets_tube')

        log.info("Initiating beanstalk connection. Queueing tweets to {use}...", use=tweets_tube)

        self.beanstalk = politwoops.utils.beanstalk(host=self.config.get('beanstalk', 'host'),
                                                    port=int(self.config.get('beanstalk', 'port')),
                                                    watch=None,
                                                    use=tweets_tube)
    
    def get_stream(self):
        queue_module = self.get_config_default('tweets-client', 'track-module', 'tweetsclient.config_track')
        queue_class = self.get_config_default('tweets-client', 'track-class', 'ConfigTrackPlugin')
        log.debug("Loading track plugin: {module} - {klass}",
                  module=queue_module, klass=queue_class)

        pluginClass = self.load_plugin(queue_module, queue_class)
        self.track = pluginClass()
        #self.track = tweetsclient.MySQLTrackPlugin({'verbose': self.verbose})
        # self.track = tweetsclient.ConfigTrackPlugin({'verbose': self.verbose})
        stream_type = self.track.get_type()
        log.debug("Initializing a {0} stream of tweets.", stream_type)
        track_items = self.track.get_items()
        log.debug(str(track_items))
        stream = None
        if stream_type == 'users':
            stream = tweetstream.FilterStream(self.user, self.passwd, track_items)
        elif stream_type == 'words':
            stream = tweetstream.TrackStream(self.user, self.passwd, track_items)
        else:
            stream = tweetstream.TweetStream(self.user, self.passwd)            
        return stream
    
    def run(self):
        self.init_beanstalk()
        log.debug("Setting up stream ...")
        stream = self.get_stream()
        log.debug("Done setting up stream ...")
        for tweet in stream:
            self.handle_tweet(stream, tweet)
        self.beanstalk.disconnect()
        return 0
 
    def handle_tweet(self, stream, tweet):
        # reset the restart counter once a tweet has come in
        self.restartCounter = 0
        # add the tweet to the queue
        log.info(u"Queued tweet {0}", tweet)
        self.beanstalk.put(anyjson.serialize(tweet))

def main(args):
    signal.signal(signal.SIGHUP, politwoops.utils.restart_process)

    log_handler = politwoops.utils.configure_log_handler(_script_, args.loglevel, args.output)
    with logbook.NullHandler():
        with log_handler.applicationbound():
            politwoops.utils.start_heartbeat_thread()
            log.debug("Starting tweets-client.py")
            try:
                app = TweetStreamClient()
                if args.restart:
                    return politwoops.utils.run_with_restart(app.run)
                else:
                    return app.run()
            except KeyboardInterrupt:
                log.notice("Killed by CTRL-C")

if __name__ == "__main__":
    args_parser = argparse.ArgumentParser(description=__doc__)
    args_parser.add_argument('--loglevel', metavar='LEVEL', type=str,
                             help='Logging level (default: notice)',
                             default='notice',
                             choices=('debug', 'info', 'notice', 'warning',
                                      'error', 'critical'))
    args_parser.add_argument('--output', metavar='DEST', type=str,
                             default='-',
                             help='Destination for log output (-, syslog, or filename)')
    args_parser.add_argument('--restart', default=False, action='store_true',
                             help='Restart when an error cannot be handled.')
    args = args_parser.parse_args()
    sys.exit(main(args))
