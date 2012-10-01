#!/user/bin/env python

import time
import datetime
import threading
import sys
import os
import signal

import logbook
import beanstalkc
import anyjson

import tweetsclient


def beanstalk(host='localhost', port=11300, watch=None, use=None):
    beanstalk = beanstalkc.Connection(host=host, port=port)
    if use:
        beanstalk.use(use)
    if watch:
        beanstalk.watch(watch)
    return beanstalk


def configure_log_handler(loglevel, output):
    if isinstance(loglevel, (str, unicode)):
        loglevel = getattr(logbook, loglevel.upper())

    if not isinstance(loglevel, int):
        raise TypeError("configure_log_handler expects loglevel to be either an integer or a string corresponding to an integer attribute of the logbook module.")

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


def start_heartbeat_thread():
    """
    Updates the access and modification timestamp of a
    file every `interval` seconds. It does so from a
    background thread, so it is only an indication that
    the script is running -- not that it's getting tweets.
    """

    config = tweetsclient.Config().get()
    try:
        heartbeat_interval = float(config.get('tweets-client', 'heartbeat_interval'))
    except:
        logbook.warning("No heartbeat_interval configuration parameter, skipping heartbeat.")
        return

    try:
        heartbeats_directory = config.get('tweets-client', 'heartbeats_directory')
    except:
        logbook.warning("No heartbeats_directory configuration parameter, skipping heartbeat.")
        return

    if not os.path.isdir(heartbeats_directory):
        logbook.warning("The heartbeats_directory parameter ({0}) is not a directory.",
                         heartbeats_directory)
        return

    scriptname = os.path.basename(sys.argv[0])
    heartbeat_filepath = os.path.join(heartbeats_directory, scriptname)
    start_time = datetime.datetime.now().isoformat()
    pid = os.getpid()

    with file(heartbeat_filepath, 'w') as fil:
        fil.write(anyjson.serialize({
            'pid': pid,
            'started': start_time
        }))

    def _heartbeat():
        while True:
            time.sleep(heartbeat_interval)
            now = datetime.datetime.now()
            try:
                stat = os.stat(heartbeat_filepath)
                mtime = datetime.datetime.fromtimestamp(stat.st_mtime)
            except OSError as e:
                if e.errno == 2: # No such file or directory
                    logbook.warning("Heartbeat file disappeared, restarting via SIGHUP.")
                    os.kill(pid, signal.SIGHUP)
                    return
                else:
                    raise

            if mtime >= now:
                logbook.warning("Heartbeat file mtime is in the future, restarting via SIGHUP.")
                os.kill(pid, signal.SIGHUP)
                return

            os.utime(heartbeat_filepath, None)

    heartbeat = threading.Thread(target=_heartbeat)
    # This causes the heartbeat thread to die with the main thread
    heartbeat.daemon = True 
    heartbeat.start()

