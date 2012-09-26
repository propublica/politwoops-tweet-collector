#!/usr/bin/env python
# encoding: utf-8
"""
Monitors a queue for tweets with link entities to take
screenshots of or image entities to be mirrored.
"""

import os
import sys
import time
import httplib
import mimetypes
import urlparse
import subprocess
import threading
import argparse
from tempfile import NamedTemporaryFile

import anyjson
import MySQLdb
import requests
import logbook
from boto.s3.connection import S3Connection
from boto.s3.key import Key

import tweetsclient
import politwoops


log = logbook.Logger(os.path.basename(__file__)
                     if __name__ == "__main__"
                     else __name__)

class PhantomJSTimeout(Exception):
    def __init__(self, cmd, process, stdout, stderr, *args, **kwargs):
        import ipdb; ipdb.set_trace()
        msg = u"phantomjs timeout for pid {process.pid}; cmd: {cmd!r} stdout: {stdout!r}, stderr: {stderr!r}".format(process=process, cmd=cmd, stdout=stdout, stderr=stderr)
        super(PhantomJSTimeout, self).__init__(msg, *args, **kwargs)
        self.cmd = cmd
        self.process = process
        

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
            response = requests.head(url, allow_redirects=True)
            log.info("HEAD {status_code} {url} {bytes}",
                     status_code=response.status_code,
                     url=url,
                     bytes=len(response.content) if response.content else '')
            if response.url not in unique_urls:
                unique_urls.append(response.url)
    return unique_urls


class TweetEntityWorker(object):
    def __init__(self):
        super(TweetEntityWorker, self).__init__()
        self.config = tweetsclient.Config().get()

    def run(self):
        mimetypes.init()
        log.debug("Initialized mime type database.")
        screenshot_tube = self.config.get('politwoops', 'screenshot_tube')
        self.beanstalk = politwoops.utils.beanstalk(
            host=self.config.get('beanstalk', 'host'),
            port=int(self.config.get('beanstalk', 'port')),
            watch=screenshot_tube,
            use=None)
        log.debug("Connected to queue.")
        self.database = MySQLdb.connect(
            host=self.config.get('database', 'host'),
            port=int(self.config.get('database', 'port')),
            db=self.config.get('database', 'database'),
            user=self.config.get('database', 'username'),
            passwd=self.config.get('database', 'password'),
            charset="utf8",
            use_unicode=True
        )
        log.debug("Connected to database.")
        self.database.autocommit(True) # needed if you're using InnoDB
        self.database.cursor().execute('SET NAMES UTF8')

        while True:
            time.sleep(0.2)
            job = self.beanstalk.reserve(timeout=0)
            if job:
                try:
                    tweet = anyjson.deserialize(job.body)
                    self.process_entities(tweet)
                    job.delete()
                except Exception as e:
                    job.bury()
                    log.error("Exception caught, burying screenshot job: {0}", e)

    def process_entities(self, tweet):
        entities = []
        if tweet['entities'].has_key('urls'):
            entities = entities + tweet['entities']['urls']
        if tweet['entities'].has_key('media'):
            entities = entities + tweet['entities']['media']

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
                response = requests.head(url)
                log.info("HEAD {status_code} {url} {bytes}",
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

    def record_tweet_image(self, tweet, url):
        cursor = self.database.cursor()
        cursor.execute("""INSERT INTO `tweet_images` (`tweet_id`, `url`, `created_at`, `updated_at`) VALUES(%s, %s, NOW(), NOW())""", (tweet['id'], url))
        log.info("Inserted image into database for tweet {tweet}: {url}",
                  tweet=tweet.get('id'), url=url)
    

    def screenshot_entity_url(self, tweet, entity_index, url):
        filename = "{tweet}-{index}.png".format(tweet=tweet.get('id'),
                                                index=entity_index)

        with NamedTemporaryFile(mode='wb', prefix='twoops', suffix='.png', delete=True) as fil:
            cmd = ["phantomjs", "js/rasterize.js", url, fil.name]
            (stdout, stderr) = run_subprocess_safely(cmd,
                                                     timeout=30,
                                                     timeout_signal=15)
            new_url = self.upload_image(fil.name, filename, 'image/png')
            if new_url:
                self.record_tweet_image(tweet, new_url)

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
                self.record_tweet_image(tweet, new_url)


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
            log.notice("Uploaded image {0} to {1}", tmp_path, url)
            return url
        except IOError as e:
            log.warn("Failed to upload image {0} to {1} because {2}", tmp_path, url, str(e))
            return None

def main(args):
    loglevel = getattr(logbook, args.loglevel.upper())
    if args.output == 'syslog':
        log_handler = logbook.SyslogHandler(
            application_name='politwoops-worker',
            bubble=False,
            level=loglevel)
    elif args.output == '-' or not args.output:
        log_handler = logbook.StderrHandler(
            level=loglevel,
            bubble=False)
    else:
        log_handler = logbook.FileHandler(
            filename=args.output,
            encoding='utf-8',
            level=loglevel,
            bubble=False)

    with logbook.NullHandler():
        with log_handler.applicationbound():
            worker = TweetEntityWorker()
            worker.run()

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
    args = args_parser.parse_args()
    sys.exit(main(args))

