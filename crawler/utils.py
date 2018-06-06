import json
import ast
import logging
import os
import pandas as pd
import re
from urllib import parse
import zipfile
from configparser import ConfigParser, RawConfigParser
from contextlib import redirect_stdout

import rarfile
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


def print_job_model(job_model: dict):
    """
    Basic Printout of Hierarchy, better looking than json.dumps()
    
    :param dict job_model: Dictionary of table names with configuration
                           information on structure, jobs, etc
    """

    for table, settings in job_model.items():
        print('\ntable:', table, '\n|')
        for category, inf in settings.items():
            print('|--' + category)
            if category in ['index_col', 'store']:
                print('|    |-' + str(inf), end='\n|\n')
                continue
            for item in inf:
                if category == list(job_model[table].keys())[-1]:
                    print('     |-' + str(item))
                else:
                    print('|    |-' + str(item))
            if category != list(job_model[table].keys())[-1]:
                print('|')


def export_hierarchy(table_hierarchy: dict, output: str = 'table_hierarchy.txt'):
    """
    Wrapper for file output of print_hierarchy method
    
    :param dict table_hierarchy: Dictionary of table names with configuration
                                 information on structure, jobs, etc
    :param str output: Path to output file.
    """

    with open(output, 'w') as f:
        with redirect_stdout(f):
            print_job_model(table_hierarchy)


def save_sources_config(data: dict, output_path: str, file_format: str = 'json'):
    """
    Save your table_hierarchy data into file.
    
    :param dict data: Table hierarchy dictionary.
    :param str output_path: Destination to save file.
    :param str file_format: Output format, can be either 'json' or 'ini'
    """

    if file_format == 'json':
        with open(output_path, 'w') as configfile:
            json.dump(data, configfile, indent=2, ensure_ascii=False)

    elif file_format in ['ini']:
        config = ConfigParser()

        for key1, data1 in data.items():
            config[key1] = {}
            for key2, data2 in data1.items():
                config[key1]["{}".format(key2)] = str(data2)

        with open(output_path, 'w') as configfile:
            config.write(configfile)

    else:
        raise ValueError("file_format can only be [ json | ini ]")


def read_sources_config(source: str, file_format: str = 'auto'):
    """
    Read table_hierarchy data into dict.
    
    :param str source: Configuration file path.
    :param str file_format: File format, can be either 'ini', 'json' or 'auto' for automatic
                            selection based on file extension.
    """

    _, file_ext = os.path.splitext(source)

    if (file_format == 'auto' and file_ext in ['.ini', '.conf']) or file_format in ['ini', 'conf']:
        parser = ConfigParser()
        parser.read(source)

        data = {}

        for section in parser.sections():
            data[section] = {}
            for k, val in parser.items(section):
                if k in ['index_col', 'store']:
                    data[section][k] = str(val)
                else:
                    data[section][k] = ast.literal_eval(str(val))

    elif (file_format == 'auto' and file_ext in ['.json']) or file_format in ['json']:
        with open(source, 'r') as configfile:
            data = json.load(configfile)

    else:
        raise ValueError("Unknown format: {}".format(file_ext))

    return data
