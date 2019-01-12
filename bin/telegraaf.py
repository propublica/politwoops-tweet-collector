#!/usr/bin/env python
# encoding: utf-8
"""
kamertweets.py

Created by Breyten Ernsting on 2010-05-31.
Copyright (c) 2010 __MyCompanyName__. All rights reserved.
"""

import sys
import os
import re
import urllib

from BeautifulSoup import BeautifulSoup

import twython

def get_telegraaf():
    url = "http://tweet.telegraaf.nl/"
    u = urllib.urlopen(url)
    t = u.read()
    u.close()
    return t

def parse_telegraaf(content):
    soup = BeautifulSoup(content)
    poliBox = soup.find('div', {'id': 'leftBarPoliticiansBox'});
    lists = poliBox.findAll('ul')
    for box in lists:
        politicians = box.findAll('a', href=re.compile('^\/'))
        profiles = {}
        for politician in politicians:
            link = politician['href']
            [partij, naam] = link.split(u'/')[1:]
            profiles[naam] = partij
    return profiles

def get_ids(names):
    twitter = twython.core.setup(username='breyten', password='a0p7i2n5')
    return twitter.bulkUserLookup(screen_names=names)

def main():
    # FIXME: misschien beter om http://kamertweets/CDA/ etc. te parsen, dan hoeft het maar 1 x per dag.
    content = get_telegraaf()
    profiles = parse_telegraaf(content)
    #print profiles
    twitter_ids = get_ids(profiles.keys())
    names2id = {}
    for user_info in twitter_ids:
        names2id[user_info['screen_name']] = user_info['id']
    print names2id.keys()
    print profiles.keys()
    for user_name in profiles.keys():
        if user_name in names2id:
            print "INSERT INTO `politicians` (`user_name`, `party`, `twitter_id`) VALUES(\"%s\", \"%s\", %s);" % (user_name, profiles[user_name], names2id[user_name])
    #print twitter_ids[0]
    #print ','.join([str(id['id']) for id in twitter_ids])

if __name__ == '__main__':
    main()
