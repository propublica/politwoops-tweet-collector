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
import configparser
from functools import reduce

import socket
# disable buffering
#socket._fileobject.default_bufsize = 0

import http.client
http.client.HTTPConnection.debuglevel = 1

import anyjson
import logbook

# this is for consuming the streaming API
import MySQLdb
import tweepy
import tweetsclient
import politwoops


_script_ = (os.path.basename(__file__)
            if __name__ == "__main__"
            else __name__)
log = logbook.Logger(_script_)

class DataRecord(object):
    def __init__(self, *args, **kwargs):
        object.__setattr__(self, '_dict', {})
        self._dict.update(((arg, None) for arg in args))
        self._dict.update(kwargs)

    def __getattr__(self, attr):
        if attr not in self._dict:
            raise AttributeError("{cls!r} has no attribute {attr!r}".format(cls=self.__class__.__name__,
                                                                        attr=attr))
        return self._dict[attr]

    def __setattr__(self, attr, value):
        raise AttributeError("All attributes of DataRecord objects are read-only")

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

def dict_mget(thedict, keylist, default=None):
    result = reduce(lambda d, k: None if d is None else d.get(k), keylist, thedict)
    return result if result is not None else default

class TweetListener(tweepy.streaming.StreamListener):
    def __init__(self, queue, *args, **kwargs):
        super(TweetListener, self).__init__(*args, **kwargs)
        self.queue = queue

    def on_data(self, data):
        try:
            tweet = anyjson.deserialize(data)
            self.queue.put(anyjson.serialize(tweet))
            if 'delete' in tweet:
                status = dict_mget(tweet, ['delete', 'status'])
                if status is not None:
                    log.notice(u"Queued delete notification for user {0} for tweet {1}".format(status.get('user_id_str'), status.get('id_str')))

            elif 'user' in tweet:
                log.notice(u"Queued tweet for user {0} for tweet {1}".format(dict_mget(tweet, ['user', 'screen_name']), tweet.get('id_str')))

            else:
                log.notice(u"Queued tweet: {0}".format(tweet))

        except Exception as e:
            log.error(u"TweetListener.on_data() caught exception: {0}".format(e))
            return False  # Closes connection, stops streaming

    def on_timeout(self):
        log.error(u"TweetListener connection timed out.")

    def on_error(self, status_code):
        log.error(u"TweetListener got bad status code: {0}".format(status_code))

class TweetStreamClient(object):
    def __init__(self):
        self.config = tweetsclient.Config().get()
        consumer_key = self.config.get('tweets-client', 'consumer_key')
        consumer_secret = self.config.get('tweets-client', 'consumer_secret')
        access_token = self.config.get('tweets-client', 'access_token')
        access_token_secret = self.config.get('tweets-client', 'access_token_secret')
        log.debug("Consumer credentials: {key}, {secret}",
                  key=consumer_key,
                  secret=consumer_secret)
        log.debug("Access credentials: {token}, {secret}",
                  token=access_token,
                  secret=access_token_secret)
        self.twitter_auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        self.twitter_auth.set_access_token(access_token, access_token_secret)
        self.database = MySQLdb.connect(
            host=self.config.get('database', 'host'),
            port=int(self.config.get('database', 'port')),
            db=self.config.get('database', 'database'),
            user=self.config.get('database', 'username'),
            passwd=self.config.get('database', 'password'),
            charset="utf8mb4",
            use_unicode=True
        )
        self.database.autocommit(True) # needed if you're using InnoDB
        self.database.cursor().execute('SET NAMES UTF8MB4')
        try:
            username = self.twitter_auth.get_username()
            log.notice("Authenticated as {user}".format(user=username))
        except tweepy.error.TweepError as e:
            log.error(e)

    def get_config_default(self, section, key, default = None):
        try:
            return self.config.get(section, key)
        except configparser.NoOptionError:
            return default

    def get_users(self):
        cursor = self.database.cursor()
        q = "SELECT `twitter_id`, `user_name`, `id` FROM `politicians` where status IN (1,2)"
        cursor.execute(q)
        ids = {}
        politicians = {}
        for t in cursor.fetchall():
            ids[str(t[0])] = t[2]
            politicians[str(t[0])] = t[1]
        log.info("Found ids: {ids}", ids=ids)
        log.info("Found politicians: {politicians}", politicians=politicians)
        return ids, politicians

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


    def stream_forever(self):
        track_module = self.get_config_default('tweets-client', 'track-module', 'tweetsclient.config_track')
        track_class = self.get_config_default('tweets-client', 'track-class', 'ConfigTrackPlugin')
        log.debug("Loading track plugin: {module} - {klass}",
                  module=track_module, klass=track_class)

        pluginClass = self.load_plugin(track_module, track_class)
        self.track = pluginClass()
        log.debug("Initializing a stream of tweets.")
        track_items = self.track.get_items()
        log.debug(str(track_items))

        stream = None
        self.users, self.politicians = self.get_users()
        log.notice("Retrieved {length} users", length=len(list(self.users.keys())))
        tweet_listener = TweetListener(self.beanstalk)
        stream = tweepy.Stream(self.twitter_auth, tweet_listener, secure=True)
        stream.filter(follow=list(self.users.keys()))

    def run(self):
        self.init_beanstalk()
        with politwoops.utils.Heart() as heart:
            politwoops.utils.start_heartbeat_thread(heart)
            politwoops.utils.start_watchdog_thread(heart)
            self.stream_forever()

        self.beanstalk.close()
        return 0

def main(args):
    signal.signal(signal.SIGHUP, politwoops.utils.restart_process)

    log_handler = politwoops.utils.configure_log_handler(_script_, args.loglevel, args.output)
    with logbook.NullHandler():
        with log_handler.applicationbound():
            log.debug("Starting tweets-client.py")
            try:
                app = TweetStreamClient()
                if args.authtest:
                    return
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
    args_parser.add_argument('--authtest', default=False, action='store_true',
                             help='Authenticate against Twitter and exit.')
    args = args_parser.parse_args()
    sys.exit(main(args))
