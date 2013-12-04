#!/usr/bin/env python
# encoding: utf-8
"""
politwoops-worker.py

Created by Breyten Ernsting on 2010-05-30.
Copyright (c) 2010 __MyCompanyName__. All rights reserved.
"""

import sys
import os
import time
import mimetypes
import argparse
import MySQLdb
import anyjson
import smtplib
import signal
import pytz
from email.mime.text import MIMEText
from datetime import datetime

import socket
# disable buffering
socket._fileobject.default_bufsize = 0

import httplib
httplib.HTTPConnection.debuglevel = 1

import urllib2
import MySQLdb
import anyjson
import logbook
import tweetsclient
import politwoops

from stathat import StatHat

_script_ = (os.path.basename(__file__)
            if __name__ == "__main__"
            else __name__)
log = logbook.Logger(_script_)

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg


class DeletedTweetsWorker(object):
    def __init__(self, heart, images):
        self.heart = heart
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
        tweets_tube = self.config.get('beanstalk', 'tweets_tube')
        screenshot_tube = self.config.get('beanstalk', 'screenshot_tube')

        log.info("Initiating beanstalk connection. Watching {watch}.", watch=tweets_tube)
        if self.images:
            log.info("Queueing screenshots to {use}.", use=screenshot_tube)

        self.beanstalk = politwoops.utils.beanstalk(host=self.config.get('beanstalk', 'host'),
                                                    port=int(self.config.get('beanstalk', 'port')),
                                                    watch=tweets_tube,
                                                    use=screenshot_tube)

    def _database_keepalive(self):
        cur = self.database.cursor()
        cur.execute("""SELECT id FROM tweets LIMIT 1""")
        cur.fetchone()
        cur.close()
        log.info("Executed database connection keepalive query.")

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
            if self.heart.beat():
                self._database_keepalive()
            reserve_timeout = max(self.heart.interval.total_seconds() * 0.1, 2)
            job = self.beanstalk.reserve(timeout=reserve_timeout)
            if job:
                self.handle_tweet(job.body)
                job.delete()

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

        cursor.execute("""SELECT * FROM `tweets` WHERE `id` = %s""", (tweet['delete']['status']['id'],))
        ref_tweet = cursor.fetchone()
        self.send_alert(ref_tweet[1], ref_tweet[4], ref_tweet[2])

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

        retweeted_id = None
        retweeted_content = None
        retweeted_user_name = None
        if tweet.has_key('retweeted_status'):
            retweeted_id = tweet['retweeted_status']['id']
            retweeted_content = tweet['retweeted_status']['text']
            retweeted_user_name = tweet['retweeted_status']['user']['screen_name']

        if num_previous > 0:
            cursor.execute("""UPDATE `tweets` SET `user_name` = %s, `politician_id` = %s, `content` = %s, `tweet`=%s, `retweeted_id`=%s, `retweeted_content`=%s, `retweeted_user_name`=%s, `modified`= NOW() WHERE id = %s""",
                           (tweet['user']['screen_name'],
                            self.users[tweet['user']['id']],
                            tweet['text'],
                            anyjson.serialize(tweet),
                            retweeted_id,
                            retweeted_content,
                            retweeted_user_name,
                            tweet['id']))
            log.info("Updated tweet {0}", tweet.get('id'))
        else:
            cursor.execute("""INSERT INTO `tweets` (`id`, `user_name`, `politician_id`, `content`, `created`, `modified`, `tweet`, retweeted_id, retweeted_content, retweeted_user_name) VALUES(%s, %s, %s, %s, NOW(), NOW(), %s, %s, %s, %s)""",
                           (tweet['id'],
                            tweet['user']['screen_name'],
                            self.users[tweet['user']['id']],
                            tweet['text'],
                            anyjson.serialize(tweet),
                            retweeted_id,
                            retweeted_content,
                            retweeted_user_name))
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


    def send_alert(self, username, created, text):
        if username and self.config.has_section('moderation-alerts'):
            host = self.config.get('moderation-alerts', 'mail_host')
            port = self.config.get('moderation-alerts', 'mail_port')
            user = self.config.get('moderation-alerts', 'mail_username')
            password = self.config.get('moderation-alerts', 'mail_password')
            recipient = self.config.get('moderation-alerts', 'twoops_recipient')
            sender = self.config.get('moderation-alerts', 'sender')

            if not text:
                #in case text is None from a deleted but not originally captured deleted tweet
                text = ''
            text += "\n\nModerate this deletion here: http://politwoops.sunlightfoundation.com/admin/review\n\nEmail the moderation group if you have questions or would like a second opinion at politwoops-moderation@sunlightfoundation.com"

            nowtime = datetime.now()
            diff = nowtime - created
            diffstr = ''
            if diff.days != 0:
                diffstr += '%s days' % diff.days
            else:
                if diff.seconds > 86400:
                    diffstr += "%s days" % (diff.seconds / 86400 )
                elif diff.seconds > 3600:
                    diffstr += "%s hours" % (diff.seconds / 3600)
                elif diff.seconds > 60:
                    diffstr += "%s minutes" % (diff.seconds / 60)
                else:
                    diffstr += "%s seconds" % diff.seconds

            nowtime = pytz.timezone('UTC').localize(nowtime)
            nowtime = nowtime.astimezone(pytz.timezone('US/Eastern'))

            smtp = smtplib.SMTP(host, port)
            smtp.login(user, password)
            msg = MIMEText(text.encode('UTF-8'), 'plain', 'UTF-8')
            msg['Subject'] = 'Politwoop! @%s -- deleted on %s after %s' % (username, nowtime.strftime('%m-%d-%Y %I:%M %p'), diffstr)
            msg['From'] = sender
            msg['To'] = recipient
            smtp.sendmail(sender, recipient, msg.as_string())


def main(args):
    signal.signal(signal.SIGHUP, politwoops.utils.restart_process)

    log_handler = politwoops.utils.configure_log_handler(_script_, args.loglevel, args.output)
    with logbook.NullHandler():
        with log_handler.applicationbound():
            try:
                log.info("Starting Politwoops worker...")
                log.notice("Log level {0}".format(log_handler.level_name))
                if args.images:
                    log.notice("Screenshot support enabled.")

                with politwoops.utils.Heart() as heart:
                    politwoops.utils.start_watchdog_thread(heart)
                    app = DeletedTweetsWorker(heart, args.images)
                    if args.restart:
                        return politwoops.utils.run_with_restart(app.run)
                    else:
                        try:
                            return app.run()
                        except Exception as e:
                            logbook.error("Unhandled exception of type {exctype}: {exception}",
                                          exctype=type(e),
                                          exception=str(e))
                            if not args.restart:
                                raise

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
    args_parser.add_argument('--images', default=False, action='store_true',
                             help='Whether to screenshot links or mirror images linked in tweets.')
    args_parser.add_argument('--restart', default=False, action='store_true',
                             help='Restart when an error cannot be handled.')

    args = args_parser.parse_args()
    sys.exit(main(args))
