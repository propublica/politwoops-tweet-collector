#!/user/bin/env python

import logbook
import beanstalkc
import time

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

