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
import mimetypes
import ConfigParser
import MySQLdb

from time import sleep

import anyjson
import beanstalkc

import socket
# disable buffering
socket._fileobject.default_bufsize = 0

import httplib
httplib.HTTPConnection.debuglevel = 1

import urllib
import urllib2
import urlparse
from tempfile import NamedTemporaryFile
import requests
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

# used for screenshot capturing and archiving
import subprocess
from boto.s3.connection import S3Connection
from boto.s3.key import Key

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg


class DeletedTweetsWorker:
    def __init__(self, verbose, output, images):
        self.verbose = verbose
        self.output = output
        self.images = images
        self.get_config()
    
    def get_database(self):
        log.debug("Making DB connection")
        conn = MySQLdb.connect(
            host=self.config.get('database', 'host'),
            port=int(self.config.get('database', 'port')),
            db=self.config.get('database', 'database'),
            user=self.config.get('database', 'username'),
            passwd=self.config.get('database', 'password'),
            charset="utf8",
            use_unicode=True
        )
        conn.autocommit(True) # needed if you're using InnoDB
        conn.cursor().execute('SET NAMES UTF8')
        return conn
    
    def get_beanstalk(self):
        tube = self.config.get('beanstalk', 'tube')
        log.debug("Initiating beanstalk connection for tube {tube}...", tube=tube)
        beanstalk = politwoops.utils.beanstalk(host=self.config.get('beanstalk', 'host'), port=int(self.config.get('beanstalk', 'port')), tube=self.config.get('beanstalk', 'tube'))
        return beanstalk

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
        log.debug("Found ids: {ids}", ids=ids)
        log.debug("Found politicians: {politicians}", politicians=politicians)
        return ids, politicians

    def run(self):
        mimetypes.init()
        log_handler = logbook.SyslogHandler(application_name='politwoops-worker',
                                            bubble=self.verbose,
                                            level='DEBUG' if self.verbose else 'INFO')
        with log_handler.applicationbound():
            self.database = self.get_database()
            self.beanstalk = self.get_beanstalk()
            self.users, self.politicians = self.get_users()
            self.stathat = self.get_stathat()
            while True:
                sleep(0.2)
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
                sleep(1) # restart after a second

                self.restartCounter += 1
                log.debug("Some sort of error, restarting for the {nth} time: {exception}",
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
                    entities = []
                    if tweet['entities'].has_key('urls'):
                        entities = entities + tweet['entities']['urls']
                    if tweet['entities'].has_key('media'):
                        entities = entities + tweet['entities']['media']

                    for entity_index, url_entity in enumerate(entities):
                        urls = [url for url in [url_entity.get('media_url'),
                                                url_entity.get('expanded_url'),
                                                url_entity.get('url')]
                                if url is not None]

                        log_context={'entity': entity_index,
                                     'tweet': tweet.get('id'),
                                     'urls': urls}

                        if len(urls) == 0:
                            log.warn("No URLs for entity {entity} on tweet {tweet}. Skipping.",
                                     **log_context)
                            return

                        log.debug("URLs for entity {entity} on tweet {tweet}: {urls}",
                                  **log_context)

                        for url in urls:
                            response = requests.head(url)
                            log.debug("HEAD {status_code} {url} {bytes}",
                                      status_code=response.status_code,
                                      url=url,
                                      bytes=len(response.content) if response.content else '')
                            if response.status_code != httplib.OK:
                                log.warn("Unable to retrieve head for entity URL {0}", url)
                                continue
                            
                            if response.headers.get('content-type', '').startswith('image/'):
                                self.mirror_entity_image(tweet, entity_index, url)
                            else:
                                self.screenshot_entity_url(tweet, entity_index, url)
                            break


    def handle_deletion(self, tweet):
        log.debug("Deleted tweet {0}", tweet['delete']['status']['id'])
        cursor = self.database.cursor()
        cursor.execute("""SELECT COUNT(*) FROM `tweets` WHERE `id` = %s""", (tweet['delete']['status']['id'],))
        num_previous = cursor.fetchone()[0]
        if num_previous > 0:
            cursor.execute("""UPDATE `tweets` SET `modified` = NOW(), `deleted` = 1 WHERE id = %s""", (tweet['delete']['status']['id'],))
            self.copy_tweet_to_deleted_table(tweet['delete']['status']['id'])
        else:
            cursor.execute("""REPLACE INTO `tweets` (`id`, `deleted`, `modified`, `created`) VALUES(%s, 1, NOW(), NOW())""", (tweet['delete']['status']['id']))
    
    def handle_new(self, tweet):
        log.debug("New tweet {tweet} from user {user_id}/{screen_name}",
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
            log.debug("Updated tweet {0}", tweet.get('id'))
        else:
            #cursor.execute("""DELETE FROM `tweets` WHERE `id` = %s""", (tweet['id'],))
            cursor.execute("""INSERT INTO `tweets` (`id`, `user_name`, `politician_id`, `content`, `created`, `modified`, `tweet`) VALUES(%s, %s, %s, %s, NOW(), NOW(), %s)""", (tweet['id'], tweet['user']['screen_name'], self.users[tweet['user']['id']], tweet['text'], anyjson.serialize(tweet)))
            log.debug("Inserted new tweet {0}", tweet.get('id'))

            
        if was_deleted:
            log.debug("Tweet deleted {0} before it came!", tweet.get('id'))
            self.copy_tweet_to_deleted_table(tweet['id'])
        
        self.stathat_add_count('tweets')
    
    def handle_image(self, tweet, url):
        cursor = self.database.cursor()
        cursor.execute("""INSERT INTO `tweet_images` (`tweet_id`, `url`, `created_at`, `updated_at`) VALUES(%s, %s, NOW(), NOW())""", (tweet['id'], url))
        log.debug("Inserted image into database for tweet {tweet}: {url}",
                  tweet=tweet.get('id'), url=url)
    
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

    def screenshot_entity_url(self, tweet, entity_index, url):
        filename = "{tweet}-{index}.png".format(tweet=tweet.get('id'),
                                                index=entity_index)
        tmp_dir = self.config.get('images', 'tmp_dir')
        tmp_path = os.path.join(tmp_dir, filename)

        command = "phantomjs js/rasterize.js %s %s" % (url, tmp_path)
        log.info("Running {0}", command)
        code = subprocess.call(command, shell=True)
        log.debug("Command {0} exited with status {1}", command, code)
        if code != 0:
            return
       
        new_url = self.upload_image(tmp_path, filename, 'image/png')
        if new_url:
            self.handle_image(tweet, new_url)

    def mirror_entity_image(self, tweet, entity_index, url):
        response = requests.get(url)
        if response.status_code != httplib.OK:
            log.warn("Failed to download image {0}", url)
            return
        content_type = response.headers.get('content-type')

        parsed_url = urlparse.urlparse(url)
        (_base, extension) = os.path.splitext(parsed_url.path)
        extension = None
        if not extension:
            extensions = [ext for ext in mimetypes.guess_all_extensions(content_type)
                          if ext != '.jpe']
            extension = extensions[0] if extensions else ''
            log.debug("Possible mime types: {0}, chose {1}", extensions, extension)
        filename = "{tweet}-{index}{extension}".format(tweet=tweet.get('id'),
                                                       index=entity_index,
                                                       extension=extension)

        with NamedTemporaryFile(mode='wb', prefix='twoops', delete=True) as fil:
            fil.write(response.content)
            fil.flush()
            new_url = self.upload_image(fil.name, filename, content_type)
            if new_url:
                self.handle_image(tweet, new_url)


    def upload_image(self, tmp_path, dest_filename, content_type):
        bucket_name = self.config.get('aws', 'bucket_name')
        access_key = self.config.get('aws', 'access_key')
        secret_access_key = self.config.get('aws', 'secret_access_key')
        url_prefix = self.config.get('aws', 'url_prefix')

        dest_path = urlparse.urljoin(url_prefix, dest_filename)
        url = 'http://s3.amazonaws.com/%s/%s' % (bucket_name, dest_path)

        conn = S3Connection(access_key, secret_access_key)
        bucket = conn.create_bucket(bucket_name)
        key = Key(bucket)
        key.key = dest_path
        try:
            key.set_contents_from_filename(tmp_path,
                                           policy='public-read',
                                           headers={'Content-Type': content_type})
            log.debug("Upload image {0} to {1}", tmp_path, url)
            return url
        except IOError as e:
            log.warn("Failed to upload image {0} to {1} because {2}", tmp_path, url, str(e))
            return None

def main(argv=None):
    verbose = False
    images = False
    output = None
    harden = True
    
    if argv is None:
        argv = sys.argv
    try:
        try:
            opts, args = getopt.getopt(argv[1:], "ho:ivr", ["help", "output=", "images", "raise"])
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
            if option in ("-i", "--images"):
                images = True
            if option in ("-r", "--raise"):
                harden = False
    
    except Usage, err:
        print >> sys.stderr, sys.argv[0].split("/")[-1] + ": " + str(err.msg)
        print >> sys.stderr, "\t for help use --help"
        return 2
    
    if verbose:
        log.info("Starting Politwoops worker...")
    if images:
        log.info("Screenshot support enabled.")

    app = DeletedTweetsWorker(verbose, output, images)
    if harden:
        return app.run_with_restart()
    else:
        return app.run()


if __name__ == "__main__":
    sys.exit(main())
