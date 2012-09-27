#!/usr/bin/env python
# encoding: utf-8
"""
config_track.py

Created by Breyten Ernsting on 2010-08-08.
Copyright (c) 2010 __MyCompanyName__. All rights reserved.
"""

import sys
import os

import tweetsclient

class ConfigTrackPlugin(tweetsclient.TrackPlugin):
    def _get_words(self):
        return self.config.get('tweets-client', 'words').split(',')
    
    def _get_users(self):
        ids = self.config.get('tweets-client', 'users').split(',')
        return ids
    
    def get_type(self):
        return self.config.get('tweets-client', 'type')
    
    def get_items(self):
        stream_type = self.get_type()
        if stream_type == 'users':
            return self._get_users()
        elif stream_type == 'words':
            return self._get_words()
        else:
            return []
