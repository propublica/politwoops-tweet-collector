#!/usr/bin/env python
# encoding: utf-8
"""
politwoops-worker.py

Created by Breyten Ernsting on 2010-05-30.
Copyright (c) 2010 __MyCompanyName__. All rights reserved.
"""

import sys
import os
import getopt
import time
import mimetypes
import MySQLdb

import anyjson

import socket
# disable buffering
socket._fileobject.default_bufsize = 0

import httplib
httplib.HTTPConnection.debuglevel = 1

import urllib2
import logbook

# external libs
sys.path.insert(0, './lib')

import tweetsclient

import politwoops

from stathat import StatHat

log = logbook.Logger(os.path.basename(__file__)
                     if __name__ == "__main__"
                     else __name__)

help_message = '''
The help message goes here.
'''





class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg


class DeletedTweetsWorker:
    def __init__(self, images):
        self.images = images
        self.get_config()
    
    def init_database(self):
        log.debug("Making DB connection")
        self.database = MySQLdb.connect(
            host=self.config.get('database', 'host'),
            port=int(self.config.get('database', 'port')),
            db=self.config.get('database', 'database'),
            user=self.config.get('database', 'username'),
            passwd=self.config.get('database', 'password'),
            charset="utf8",
            use_unicode=True
        )
        self.database.autocommit(True) # needed if you're using InnoDB
        self.database.cursor().execute('SET NAMES UTF8')
    
    def init_beanstalk(self):
        tweets_tube = self.config.get('politwoops', 'tweets_tube')
        screenshot_tube = self.config.get('politwoops', 'screenshot_tube')

        log.info("Initiating beanstalk connection. Watching {watch}, queueing screenshots to {use}...", watch=tweets_tube, use=screenshot_tube)

        self.beanstalk = politwoops.utils.beanstalk(host=self.config.get('beanstalk', 'host'),
                                                    port=int(self.config.get('beanstalk', 'port')),
                                                    watch=tweets_tube,
                                                    use=screenshot_tube)

    def get_config(self):
        log.debug("Reading config ...")
        self.config = tweetsclient.Config().get()

    def get_users(self):
        cursor = self.database.cursor()
        q = "SELECT `twitter_id`, `user_name`, `id` FROM `politicians`"
        cursor.execute(q)
        ids = {}
        politicians = {}
        for t in cursor.fetchall():
            ids[t[0]] = t[2]
            politicians[t[0]] = t[1]
        log.info("Found ids: {ids}", ids=ids)
        log.info("Found politicians: {politicians}", politicians=politicians)
        return ids, politicians

    def run(self):
        mimetypes.init()
        self.init_database()
        self.init_beanstalk()
        self.users, self.politicians = self.get_users()
        self.stathat = self.get_stathat()
        while True:
            time.sleep(0.2)
            job = self.beanstalk.reserve(timeout=0)
            if job:
                self.handle_tweet(job.body)
                job.delete()

    def run_with_restart(self):
        # keeps tabs on whether we should restart the connection to Twitter ourselves
        shouldRestart = True
        # keeps tabs on how many times we've unsuccesfully restarted -- more means longer waiting times
        self.restartCounter = 0
        
        while shouldRestart:
            shouldRestart = False
            try:
                self.run()
            except Exception as e:
                shouldRestart = True
                time.sleep(1) # restart after a second

                self.restartCounter += 1
                log.error("Some sort of error, restarting for the {nth} time: {exception}",
                          nth=self.restartCounter,
                          exception=str(e))
    
    def get_stathat(self):
        stathat_enabled = (self.config.get('stathat', 'enabled') == 'yes')
        if not stathat_enabled:
            log.debug('Running without stathat ...')
            return
        else:
            log.debug('StatHat ingeration enabled ...')
            return StatHat()
        
    def stathat_add_count(self, stat_name):
        if self.stathat is not None:
            try:
                self.stathat.ez_post_count(self.config.get('stathat', 'email'), stat_name, 1)
            except urllib2.URLError, e:
                pass
            except urllib2.HTTPError, e:
                pass
    
    def handle_tweet(self, job_body):
        tweet = anyjson.deserialize(job_body)
        if tweet.has_key('delete'):
            if tweet['delete']['status']['user_id'] in self.users.keys():
                self.handle_deletion(tweet)
        else:
            if tweet.has_key('user') and (tweet['user']['id'] in self.users.keys()):
                self.handle_new(tweet)

                if self.images and tweet.has_key('entities'):
                    # Queue the tweet for screenshots and/or image mirroring
                    log.notice("Queued tweet {0} for entity archiving.", tweet['id'])
                    self.beanstalk.put(anyjson.serialize(tweet))


    def handle_deletion(self, tweet):
        log.notice("Deleted tweet {0}", tweet['delete']['status']['id'])
        cursor = self.database.cursor()
        cursor.execute("""SELECT COUNT(*) FROM `tweets` WHERE `id` = %s""", (tweet['delete']['status']['id'],))
        num_previous = cursor.fetchone()[0]
        if num_previous > 0:
            cursor.execute("""UPDATE `tweets` SET `modified` = NOW(), `deleted` = 1 WHERE id = %s""", (tweet['delete']['status']['id'],))
            self.copy_tweet_to_deleted_table(tweet['delete']['status']['id'])
        else:
            cursor.execute("""REPLACE INTO `tweets` (`id`, `deleted`, `modified`, `created`) VALUES(%s, 1, NOW(), NOW())""", (tweet['delete']['status']['id']))
    
    def handle_new(self, tweet):
        log.notice("New tweet {tweet} from user {user_id}/{screen_name}",
                  tweet=tweet.get('id'),
                  user_id=tweet.get('user', {}).get('id'),
                  screen_name=tweet.get('user', {}).get('screen_name'))

        self.handle_possible_rename(tweet)
        cursor = self.database.cursor()
        cursor.execute("""SELECT COUNT(*), `deleted` FROM `tweets` WHERE `id` = %s""", (tweet['id'],))
        
        info = cursor.fetchone()
        num_previous = info[0]
        if info[1] is not None:
            was_deleted = (int(info[1]) == 1)
        else:
            was_deleted = False
        # cursor.execute("""SELECT COUNT(*) FROM `tweets`""")
        # total_count = cursor.fetchone()[0]
        # self._debug("Total count in table: %s" % total_count)


        if num_previous > 0:
            cursor.execute("""UPDATE `tweets` SET `user_name` = %s, `politician_id` = %s, `content` = %s, `tweet`=%s, `modified`= NOW() WHERE id = %s""", (tweet['user']['screen_name'], self.users[tweet['user']['id']], tweet['text'], anyjson.serialize(tweet), tweet['id'],))
            log.info("Updated tweet {0}", tweet.get('id'))
        else:
            #cursor.execute("""DELETE FROM `tweets` WHERE `id` = %s""", (tweet['id'],))
            cursor.execute("""INSERT INTO `tweets` (`id`, `user_name`, `politician_id`, `content`, `created`, `modified`, `tweet`) VALUES(%s, %s, %s, %s, NOW(), NOW(), %s)""", (tweet['id'], tweet['user']['screen_name'], self.users[tweet['user']['id']], tweet['text'], anyjson.serialize(tweet)))
            log.info("Inserted new tweet {0}", tweet.get('id'))

            
        if was_deleted:
            log.warn("Tweet deleted {0} before it came!", tweet.get('id'))
            self.copy_tweet_to_deleted_table(tweet['id'])
        
        self.stathat_add_count('tweets')
    
    def copy_tweet_to_deleted_table(self, tweet_id):
        cursor = self.database.cursor()
        cursor.execute("""INSERT IGNORE INTO `deleted_tweets` SELECT * FROM `tweets` WHERE `id` = %s AND `content` IS NOT NULL""" % (tweet_id))
        self.stathat_add_count('deleted tweets')
        
    def handle_possible_rename(self, tweet):
        tweet_user_name = tweet['user']['screen_name']
        tweet_user_id = tweet['user']['id']
        current_user_name = self.politicians[tweet_user_id]
        if current_user_name != tweet_user_name:
            self.politicians[tweet_user_id] = tweet_user_name
            cursor= self.database.cursor()
            cursor.execute("""UPDATE `politicians` SET `user_name` = %s WHERE `id` = %s""", (tweet_user_name, self.users[tweet_user_id]))
    
    
    # screenshot capturing/archiving functionality


def main(argv=None):
    loglevel = logbook.NOTICE
    loglevel_name = "NOTICE"
    images = False
    output = None
    harden = True
    
    if argv is None:
        argv = sys.argv
    try:
        try:
            opts, args = getopt.getopt(argv[1:], "hl:o:ivr", ["help", "loglevel=", "output=", "images", "raise"])
        except getopt.error, msg:
            raise Usage(msg)
        
        # option processing
        for option, value in opts:
            if option == "-v":
                raise Usage("The verbose option (-v) is no longer supported. Use the loglevel option (-l) instead.")
            if option in ("-h", "--help"):
                raise Usage(help_message)
            if option in ("-l", "--loglevel"):
                loglevel_name = value.upper()
                if not hasattr(logbook, loglevel_name):
                    raise Usage("Invalid {0} value: {1}".format(option, value))
                loglevel = getattr(logbook, loglevel_name)
                if not isinstance(loglevel, int):
                    raise Usage("Invalid {0} value: {1}".format(option, value))
            if option in ("-o", "--output"):
                output = value
            if option in ("-i", "--images"):
                images = True
            if option in ("-r", "--raise"):
                harden = False
    
    except Usage, err:
        print >> sys.stderr, sys.argv[0].split("/")[-1] + ": " + str(err.msg)
        print >> sys.stderr, "\t for help use --help"
        return 2

    if output == 'syslog':
        log_handler = logbook.SyslogHandler(
            application_name='politwoops-worker',
            bubble=False,
            level=loglevel)
    elif output == '-' or not output:
        log_handler = logbook.StderrHandler(
            level=loglevel,
            bubble=False)
    else:
        log_handler = logbook.FileHandler(
            filename=output,
            encoding='utf-8',
            level=loglevel,
            bubble=False)

    with logbook.NullHandler():
        with log_handler.applicationbound():
            log.info("Starting Politwoops worker...")
            log.notice("Log level {0}".format(loglevel_name))
            if images:
                log.notice("Screenshot support enabled.")

            app = DeletedTweetsWorker(images)
            if harden:
                return app.run_with_restart()
            else:
                return app.run()


if __name__ == "__main__":
    sys.exit(main())
