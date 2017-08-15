#!/usr/bin/env python
# encoding: utf-8
"""
Monitors a queue for tweets with link entities to take
screenshots of or image entities to be mirrored.
"""

import os
import sys
import re
import time
import httplib
import mimetypes
import urlparse
import subprocess
import threading
import argparse
import signal
import contextlib
from tempfile import NamedTemporaryFile

import anyjson
import MySQLdb
import requests
import logbook
from boto.s3.connection import S3Connection
from boto.s3.key import Key

import tweetsclient
import politwoops
from politwoops.utils import dict_mget


_script_ = (os.path.basename(__file__)
            if __name__ == "__main__"
            else __name__)
log = logbook.Logger(_script_)


class PhantomJSTimeout(Exception):
    def __init__(self, cmd, process, stdout, stderr, *args, **kwargs):
        msg = u"phantomjs timeout for pid {process.pid}; cmd: {cmd!r} stdout: {stdout!r}, stderr: {stderr!r}".format(process=process, cmd=cmd, stdout=stdout, stderr=stderr)
        super(PhantomJSTimeout, self).__init__(msg, *args, **kwargs)
        self.cmd = cmd
        self.process = process


def ensure_phantomjs_is_runnable():
    """
    phantomjs must be on the PATH environment variable. This
    tries to run it and log the version, raising an error if
    it fails.
    """
    try:
        process = subprocess.Popen(args=['phantomjs', '--version'],
                                   stdin=None,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
    except OSError as e:
        if e.errno == 2: # No such file or directory
            log.critical("Unable to find phantomjs")
            sys.exit(1)

    process.wait()
    stdout = process.stdout.read()
    stderr = process.stderr.read()

    if process.returncode != 0:
        log.critical("Unable to execute phantomjs --version: {stdout!r} {stderr!r}",
                     stdout=stdout, stderr=stderr)
        sys.exit(1)

    match = re.match('^\d+\.\d+\.\d+$', stdout.strip())
    if match is None:
        log.critical("Unrecognized version of phantomjs: {stdout!r}", stdout)
        sys.exit(1)

    log.notice("Found phantomjs version {version}", version=match.group())


def run_subprocess_safely(args, timeout=300, timeout_signal=9):
    """
    args: sequence of args, see Popen docs for shell=False
    timeout: maximum runtime in seconds (fractional seconds allowed)
    timeout_signal: signal to send to the process if timeout elapses
    """
    log.debug(u"Starting command: {0}", args)

    process = subprocess.Popen(args=args,
                               stdin=None,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)

    deadline_timer = threading.Timer(timeout, process.send_signal, args=(timeout_signal,))
    deadline_timer.start()

    stdout = ""
    stderr = ""
    start_time = time.time()
    elapsed = 0

    # In practice process.communicate() never seems to return
    # until the process exits but the documentation implies that
    # it can because EOF can be reached before the process exits.
    while not timeout or elapsed < timeout:
        (stdout1, stderr1) = process.communicate()
        if stdout1:
            stdout += stdout1
        if stderr1:
            stderr += stderr1
        elapsed = time.time() - start_time
        if process.poll() is not None:
            break

    if elapsed >= timeout:
        log.warning(u"Process failed to complete within {0} seconds. Return code: {1}", timeout, process.returncode)
        raise PhantomJSTimeout(args, process, stdout, stderr)
    else:
        deadline_timer.cancel()
        log.notice(u"Process completed in {0} seconds with return code {1}: {2} (stdout: {3!r}) (stderr: {4!r})", elapsed, process.returncode, args, stdout, stderr)
        return (stdout, stderr)


def reduce_url_list(urls):
    unique_urls = []
    for url in urls:
        if url not in unique_urls:
            try:
                response = requests.head(url, allow_redirects=True, timeout=15)
                log.info("HEAD {status_code} {url} {bytes}",
                         status_code=response.status_code,
                         url=url,
                         bytes=len(response.content) if response.content else '')
                if response.url not in unique_urls:
                    unique_urls.append(response.url)
            except requests.exceptions.SSLError as e:
                log.warning("Unable to make a HEAD request for {url} because: {e}",
                            url=url, e=e)

    return unique_urls


@contextlib.contextmanager
def database_cursor(**connect_params):
    database = MySQLdb.connect(**connect_params)
    log.debug("Connected to database.")
    database.autocommit(True) # needed if you're using InnoDB
    cursor = database.cursor()
    cursor.execute('SET NAMES UTF8MB4')
    yield cursor
    cursor.close()
    database.close()
    log.debug("Disconnected from database.")


class TweetEntityWorker(object):
    def __init__(self, heart):
        super(TweetEntityWorker, self).__init__()
        self.heart = heart
        self.config = tweetsclient.Config().get()
        self.db_connect_params = {
            'host': self.config.get('database', 'host'),
            'port': int(self.config.get('database', 'port')),
            'db': self.config.get('database', 'database'),
            'user': self.config.get('database', 'username'),
            'passwd': self.config.get('database', 'password'),
            'charset': "utf8mb4",
            'use_unicode': True
        }

    def run(self):
        mimetypes.init()
        log.debug("Initialized mime type database.")
        screenshot_tube = self.config.get('beanstalk', 'screenshot_tube')
        self.beanstalk = politwoops.utils.beanstalk(
            host=self.config.get('beanstalk', 'host'),
            port=int(self.config.get('beanstalk', 'port')),
            watch=screenshot_tube,
            use=None)
        log.debug("Connected to queue.")

        while True:
            time.sleep(0.2)
            self.heart.beat()
            reserve_timeout = max(self.heart.interval.total_seconds() * 0.1, 2)
            job = self.beanstalk.reserve(timeout=reserve_timeout)
            if job:
                try:
                    tweet = anyjson.deserialize(job.body)
                    self.process_entities(tweet)
                    job.delete()
                except Exception as e:
                    log.error("Exception caught, burying screenshot job for tweet {tweet}: {e_type} {e}",
                              tweet=tweet.get('id'), e=e, e_type=type(e))
                    job.bury()

    def process_entities(self, tweet):
        entities = []
        entities_key = 'extended_entities' if 'extended_entities' in tweet else 'entities'
        entities += dict_mget(tweet, entities_key, 'urls') or []
        entities += dict_mget(tweet, entities_key, 'media') or []

        for entity_index, url_entity in enumerate(entities):
            urls = reduce_url_list([url for url in [url_entity.get('media_url'),
                                                    url_entity.get('expanded_url'),
                                                    url_entity.get('url')]
                                    if url is not None])

            log_context={'entity': entity_index,
                         'tweet': tweet.get('id'),
                         'urls': urls}

            if len(urls) == 0:
                log.info("No URLs for entity {entity} on tweet {tweet}. Skipping.",
                         **log_context)
                return

            log.info("URLs for entity {entity} on tweet {tweet}: {urls}",
                      **log_context)

            for url in urls:
                response = requests.head(url, allow_redirects=True, timeout=15)
                log.info("HEAD {status_code} {url} {bytes}",
                          status_code=response.status_code,
                          url=url,
                          bytes=len(response.content) if response.content else '')
                if response.status_code != httplib.OK:
                    log.warn("Unable to retrieve head for tweet {tweet} entity URL {url}",
                             url=url,
                             tweet=tweet.get('id'))

                if response.headers.get('content-type', '').startswith('image/'):
                    self.mirror_entity_image(tweet, entity_index, url)
                else:
                    self.screenshot_entity_url(tweet, entity_index, url)
                break

    def record_tweet_image(self, tweet, url):
        with database_cursor(**self.db_connect_params) as cursor:
            cursor.execute("""INSERT INTO `tweet_images` (`tweet_id`, `url`, `created_at`, `updated_at`) VALUES(%s, %s, NOW(), NOW())""", (tweet['id'], url))
            log.info("Inserted image into database for tweet {tweet}: {url}",
                      tweet=tweet.get('id'), url=url)


    def screenshot_entity_url(self, tweet, entity_index, url):
        filename = "{tweet}-{index}.png".format(tweet=tweet.get('id'),
                                                index=entity_index)

        with NamedTemporaryFile(mode='wb', prefix='twoops', suffix='.png', delete=True) as fil:
            cmd = ["phantomjs", "--ssl-protocol=any", "js/rasterize.js", url, fil.name]
            (stdout, stderr) = run_subprocess_safely(cmd,
                                                     timeout=30,
                                                     timeout_signal=15)
            new_url = self.upload_image(fil.name, filename, 'image/png')
            if new_url:
                self.record_tweet_image(tweet, new_url)

    def mirror_entity_image(self, tweet, entity_index, url):
        response = requests.get(url, allow_redirects=True, timeout=15)
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
                self.record_tweet_image(tweet, new_url)


    def upload_image(self, tmp_path, dest_filename, content_type):
        bucket_name = self.config.get('aws', 'bucket_name')
        access_key = self.config.get('aws', 'access_key')
        secret_access_key = self.config.get('aws', 'secret_access_key')
        url_prefix = self.config.get('aws', 'url_prefix')

        dest_path = os.path.join(url_prefix, dest_filename)
        url = 'http://s3.amazonaws.com/%s/%s' % (bucket_name, dest_path)

        conn = S3Connection(access_key, secret_access_key)
        bucket = conn.create_bucket(bucket_name)
        key = Key(bucket)
        key.key = dest_path
        try:
            key.set_contents_from_filename(tmp_path,
                                           policy='public-read',
                                           headers={'Content-Type': content_type,
                                                    'Max-Age': 604800 })
            log.notice("Uploaded image {0} to {1}", tmp_path, url)
            return url
        except IOError as e:
            log.warn("Failed to upload image {0} to {1} because {2}", tmp_path, url, str(e))
            return None

def main(args):
    signal.signal(signal.SIGHUP, politwoops.utils.restart_process)

    log_handler = politwoops.utils.configure_log_handler(_script_, args.loglevel, args.output)
    with logbook.NullHandler():
        with log_handler.applicationbound():
            try:
                log.notice("Log level {0}".format(log_handler.level_name))
                ensure_phantomjs_is_runnable()

                with politwoops.utils.Heart() as heart:
                    politwoops.utils.start_watchdog_thread(heart)
                    worker = TweetEntityWorker(heart)
                    if args.restart:
                        politwoops.utils.run_with_restart(worker.run)
                    else:
                        worker.run()
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
