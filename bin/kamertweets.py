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

def get_kamertweets():
    url = "http://kamertweets.nl/"
    u = urllib.urlopen(url)
    t = u.read()
    u.close()
    return t

def parse_kamertweets(content):
    soup = BeautifulSoup(content)
    tweets = soup.findAll('div', {'class': 'tweet'})
    profiles = {}
    for tweet in tweets:
        link = tweet.find('a')
        partij = u' '.join(tweet.p.span.a.findAll(text=True))
        profiles[link['href']] = partij
    return profiles

def main():
    # FIXME: misschien beter om http://kamertweets/CDA/ etc. te parsen, dan hoeft het maar 1 x per dag.
    content = get_kamertweets()
    profiles = parse_kamertweets(content)
    print profiles

if __name__ == '__main__':
    main()

