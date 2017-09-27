#!/usr/bin/env python

import os
import sys
import re
import ConfigParser

class Config:
    class __impl:
        """ Implementation of the singleton interface """
        def __init__(self):
            self.config = None

        def get(self, environment='development'):
            """ Load and parses config if necessary """
            if self.config is None:
                self.config = ConfigParser.ConfigParser()
                self.config.read(['conf/tweets-client.ini'])
            return self.config

    __instance = None
    
    def __init__(self):
        """ Create singleton instance """
        # Check whether we already have an instance
        if Config.__instance is None:
            # Create and remember instance
            Config.__instance = Config.__impl()

    def get(self, environment='development'):
        return Config.__instance.get(environment)
