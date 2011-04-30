#!/user/bin/env python

import beanstalkc

def beanstalk(host='localhost', port=11300, tube='politwoops'):
    beanstalk = beanstalkc.Connection(host=host, port=port)
    beanstalk.use(tube)
    beanstalk.watch(tube)
    return beanstalk
