import ast
import logging
import os
from configparser import RawConfigParser, ConfigParser

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


def get_bot_user_token(conf_file='slack.ini'):
    # fetches bot user token
    parser = ConfigParser()
    parser.read(conf_file)
    return parser['slack']['SLACK_BOT_USER_TOKEN'], parser['slack']['channel']


def filter_log_count(log_cnt, severity=('ERROR', 'WARNING')):
    new_log_count = {}
    for key, value in log_cnt.items():
        if key in severity:
            new_log_count.update({key: value})

    if len(new_log_count) != 0:
        count_msg = "```" + \
                    "\n".join(["{}: {}".format(l, c) for l, c in new_log_count.items()]) + \
                    "```\n"
    else:
        count_msg = ""

    return count_msg


class MsgCounterHandler(logging.Handler):
    level2count = None

    def __init__(self, *args, **kwargs):
        super(MsgCounterHandler, self).__init__(*args, **kwargs)
        self.level2count = {}

    def emit(self, record):
        lvl = record.levelname
        if lvl not in self.level2count:
            self.level2count[lvl] = 0
        self.level2count[lvl] += 1


class DummySlackClient(object):
    def __init__(self):
        pass

    def api_call(self, *args, **kwargs):
        pass
