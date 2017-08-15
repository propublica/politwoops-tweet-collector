#!/user/bin/env python

import time
import datetime
import threading
import sys
import os
import re
import signal
import copy
from traceback import print_exception

import logbook
import beanstalkc
import anyjson

import tweetsclient


def dict_mget(subject, *keys, **kwargs):
    curr = subject
    for k in keys:
        try:
            if k in curr:
                curr = curr[k]
            else:
                return None
        except TypeError:
            return None
    return curr


def replace_highpoints(subject, replacement=u'\ufffd'):
    try:
        return re.sub(u'[\U00010000-\U0010ffff]', replacement, subject, re.U)
    except re.error:
        return re.sub(u'[\uD800-\uDBFF][\uDC00-\uDFFF]', replacement, subject, re.U)


def beanstalk(host='localhost', port=11300, watch=None, use=None):
    beanstalk = beanstalkc.Connection(host=host, port=port)
    if use:
        beanstalk.use(use)
    if watch:
        beanstalk.watch(watch)
    return beanstalk


def configure_log_handler(application_name, loglevel, output):
    if isinstance(loglevel, (str, unicode)):
        loglevel = getattr(logbook, loglevel.upper())

    if not isinstance(loglevel, int):
        raise TypeError("configure_log_handler expects loglevel to be either an integer or a string corresponding to an integer attribute of the logbook module.")

    if output == 'syslog':
        log_handler = logbook.SyslogHandler(
            application_name=application_name,
            facility='user',
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

    return log_handler


def run_with_restart(fn, max_restart=0, args=(), kwargs={}):
    restartCounter = 0
    while True:
        try:
            return apply(fn, args, kwargs)
        except AssertionError:
            raise
        except Exception as e:
            logbook.error("Unhandled exception of type {exctype}: {exception}",
                          exctype=type(e),
                          exception=str(e))

            restartCounter += 1
            if max_restart and restartCounter > max_restart:
                logbook.critical("Alreadying restarted {nth} times. Exiting.",
                                 nth=restartCounter)
            else:
                # Sleep longer each time we restart, but no more than 5 minutes
                delay = min(restartCounter * 15, 300)
                logbook.error("Restarting for {nth} time in {sec} seconds.",
                              nth=restartCounter,
                              sec=delay)
                time.sleep(delay)


def restart_process(signum, frame):
    """
    Replaces the current process with a new process invoked
    using the same command line.
    """
    os.execl(sys.executable, sys.executable, *sys.argv)


def start_heartbeat_thread(heart):
    """
    Triggers a regular heartbeat from a background thread
    for scripts that making blocking calls and thus can't
    heartbeat from their main loop.
    """
    def _heartbeat():
        while True:
            heart.sleep()
            heart.beat()

    heartbeat = threading.Thread(target=_heartbeat)
    # This causes the heartbeat thread to die with the main thread
    heartbeat.daemon = True
    heartbeat.start()


def start_watchdog_thread(heart):
    """
    Watch a heartbeat file and restart when the file mtime is either
    too old or too far in the future.
    """

    def _watchdog():
        while True:
            time.sleep(heart.interval.total_seconds() * 0.10)
            try:
                stat = os.stat(heart.filepath)
                mtime = datetime.datetime.fromtimestamp(stat.st_mtime)
            except OSError as e:
                if e.errno == 2: # No such file or directory
                    logbook.warning("Heartbeat file disappeared, restarting via SIGHUP.")
                    os.kill(heart.pid, signal.SIGHUP)
                    return
                else:
                    raise

            now = datetime.datetime.now()
            if mtime >= now:
                logbook.warning("Heartbeat file mtime is in the future, restarting via SIGHUP.")
                os.kill(heart.pid, signal.SIGHUP)
                return

    watchdog = threading.Thread(target=_watchdog)
    # This causes the watchdog thread to die with the main thread
    watchdog.daemon = True
    watchdog.start()


class Heart(object):
    """
    Updates the access and modification timestamp of a
    file every `interval` seconds.
    """
    def __init__(self):
        self.last_beat = datetime.datetime.now()

        config = tweetsclient.Config().get()
        try:
            self.interval = datetime.timedelta(seconds=float(config.get('tweets-client', 'heartbeat_interval')))
        except:
            logbook.warning("No heartbeat_interval configuration parameter, skipping heartbeat.")
            raise StopIteration

        try:
            directory = config.get('tweets-client', 'heartbeats_directory')
        except:
            logbook.warning("No heartbeats_directory configuration parameter, skipping heartbeat.")
            raise StopIteration

        if not os.path.isdir(directory):
            logbook.warning("The heartbeats_directory parameter ({0}) is not a directory.",
                             directory)
            raise StopIteration

        scriptname = os.path.basename(sys.argv[0])
        self.filepath = os.path.join(directory, scriptname)

        start_time = datetime.datetime.now().isoformat()
        self.pid = os.getpid()
        with file(self.filepath, 'w') as fil:
            fil.write(anyjson.serialize({
                'pid': self.pid,
                'started': start_time
            }))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if ((exc_type, exc_value, traceback) == (None, None, None)) or (exc_type is KeyboardInterrupt):
            os.unlink(self.filepath)
        else:
            with file(self.filepath, 'w') as outf:
                print_exception(exc_type, exc_value, traceback, 1000, outf)

    def sleep(self):
        while True:
            now = datetime.datetime.now()
            since = now - self.last_beat
            if since >= self.interval:
                return
            else:
                time.sleep(self.interval.total_seconds() * 0.10)

    def beat(self):
        now = datetime.datetime.now()
        since = now - self.last_beat
        if since >= self.interval:
            os.utime(self.filepath, None)
            self.last_beat = now
            return True
        else:
            return False
