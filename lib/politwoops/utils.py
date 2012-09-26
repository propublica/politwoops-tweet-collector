#!/user/bin/env python

import beanstalkc

def beanstalk(host='localhost', port=11300, watch=None, use=None):
    beanstalk = beanstalkc.Connection(host=host, port=port)
    if use:
        beanstalk.use(use)
    if watch:
        beanstalk.watch(watch)
    return beanstalk
