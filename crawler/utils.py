import ast
import logging
import os
from configparser import RawConfigParser

import requests


def make_log_dir(conf_file: str):
    """
    Helper function for logging.config.fileConfig() method. Creates any
    directories necessary for the FileHandler class.
    
    To use, run before logging.config.fileConfig(conf_file)
    
    :param str conf_file: Path to parameter file with logging configuration.
    """

    parser = RawConfigParser()
    parser.read(conf_file)

    for section in parser.sections():
        for k, val in parser.items(section):
            if k == 'class' and val == 'FileHandler':
                for log_path in ast.literal_eval(str(parser[section]['args']))[:1]:
                    os.makedirs(os.path.dirname(log_path), exist_ok=True)

def internet_on():
    try:
        requests.get('http://216.58.192.142')
        return True
    except requests.exceptions.ConnectionError:
        return False


class MsgCounterHandler(logging.Handler):
    level2count = None

    def __init__(self, *args, **kwargs):
        super(MsgCounterHandler, self).__init__(*args, **kwargs)
        self.level2count = {}

    def emit(self, record):
        l = record.levelname
        if (l not in self.level2count):
            self.level2count[l] = 0
        self.level2count[l] += 1