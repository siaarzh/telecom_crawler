import ast
import importlib
import json
import logging
import os
from configparser import ConfigParser
from contextlib import redirect_stdout

import pandas as pd
import re
import zipfile
from urllib import parse

import rarfile
import requests


def create_or_update(jobspec_dir: str = 'jobspecs', job_dir: str = 'jobs'):
    """
    Given a job.py file in jobspecs/ directory, creates a job model in
    /jobs directory of same name job.ini. Existing job.ini will be
    overwritten.

    jobspecs/job.py should have a create() method with job_model dict as
    return. An example of such a job file can be found in
    jobspecs/job.py.template

    :return: message showing create job status
    :rtype: dict
    """
    if len(os.listdir(jobspec_dir)) == 0:
        return {'success': False,
                'message': 'No jobs specified.'}

    os.makedirs(job_dir, exist_ok=True)

    try:
        for job_name in os.listdir(jobspec_dir):
            if job_name.endswith(".py") and '__init__' not in job_name:
                job_name = os.path.splitext(job_name)[0]
                job = importlib.import_module("{}.{}".format(jobspec_dir, job_name))
                job_model = job.create()
                save_job_model(job_model, os.path.join(job_dir, job_name + ".ini"), 'ini')
        return {'success': True,
                'message': None}
    except Exception as e:
        return {'success': False,
                'message': e}


def clear(job_dir: str = 'jobs'):
    """
    Remove all jobs from queue. Basically deletes all job.ini files in
    /jobs. Thus the crawler will not do any work.
    """
    for job_name in os.listdir(job_dir):
        if job_name.endswith((".ini", ".json")):
            os.remove(os.path.join(job_dir, job_name))


def queue_jobs(job_dir: str = 'jobs'):
    _job_queue = {}
    try:
        for job_name in os.listdir(job_dir):
            if job_name.endswith((".ini", ".json")):
                job = read_job_model(os.path.join(job_dir, job_name))
                _job_queue.update(job)
    except Exception as e:
        raise e

    return _job_queue


def retrieve_file_object(url: str):
    """
    Retrieves attached file names from URL and returns a GET request result

    :param str url: URL source of presumed downloadable content
    :return:    tuple (result, filename)
        WHERE
        requests.models.Response result
        str filename is the name of the attachment with extension
    """
    result = requests.get(url, verify=False, stream=True)
    try:
        cont_disp = parse.unquote(result.headers["content-disposition"])
        if re.search("UTF-8''(.*);", cont_disp) is not None:
            file_name = re.findall("UTF-8''(.*);", cont_disp)[0]
        else:
            file_name = re.findall('filename="(.+)"', cont_disp)[0]

    except NameError as e:
        raise e
    except Exception as e:
        raise e

    return result, file_name


def download_extract_files(job_queue: dict, logger_name: str = 'crawler'):
    """
    Download files and extract any xls file in archives. Return file paths list. Delete RAR/ZIPs.
    """

    logger = logging.getLogger(logger_name)

    for table, table_info in job_queue.items():
        # 1. Iterate through tables
        job_queue[table]["path"] = []  # reset paths
        temp_sheet = []  # temporary sheet number selector
        temp_skip_row = []  # temporary skip row selector
        for i, url in enumerate(table_info["urls"]):
            # 2. Iterate through urls for each table and download
            result, file_name = retrieve_file_object(url)
            file_path = os.path.join(table_info["store"], file_name)
            # file_name = file_name.encode('utf-8').decode('utf-8')
            logger.debug('{}: Downloading {}'.format(table, file_name))
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'wb') as f:
                f.write(result.content)

            _, file_ext = os.path.splitext(file_path)

            # 3. (Extract and) Append path to spreadsheet
            if file_ext in (".xls", ".xlsx"):

                logger.debug('{}: Saving {}'.format(table, file_name))
                job_queue[table]["path"].append(file_path)
                temp_sheet.append(job_queue[table]["sheet"][i])
                temp_skip_row.append(job_queue[table]["skip_row"][i])

            elif file_ext in (".rar", ".zip"):
                if file_ext == ".rar":
                    logger.debug("{}: Checking contents of {}".format(table, file_name))
                    archive = rarfile.RarFile(file_path)
                elif file_ext == ".zip":
                    logger.debug("{}: Checking contents of {}".format(table, file_name))
                    archive = zipfile.ZipFile(file_path)

                for idx, f in enumerate(archive.namelist()):
                    # check for excel and extract
                    _, f_ext = os.path.splitext(f)
                    if f_ext in (".xls", ".xlsx"):
                        archive.extract(f, table_info["store"])
                        logger.debug("{}: Saving {} (from {})".format(table, f, file_name))
                        job_queue[table]["path"].append(os.path.join(table_info["store"], f))
                        temp_sheet.append(job_queue[table]["sheet"][i])
                        temp_skip_row.append(job_queue[table]["skip_row"][i])

                logger.debug("{}: Removing {}".format(table, file_name))
                try:
                    archive.close()
                    os.remove(file_path)
                except Exception as e:
                    logger.warning("{}: Could not delete {}\n"
                                   "{}".format(table, file_name, e))

            else:
                logger.error("{}: Did not recognize downloaded file extension: {}".format(table, file_name))

        job_queue[table]["sheet"] = temp_sheet
        job_queue[table]["skip_row"] = temp_skip_row

    return job_queue


def prepare_data(table_data: dict, table_name: str, logger_name: str = 'crawler'):
    """
    Iterate through all files and load into single dataframe

    NOTE: If one url source was an archive with multiple spreadsheets
          then the options sheet and skip_row are valid for ALL those
          spreadsheets


    :param dict table_data: Dictionary containing single table's
                            structure, source paths
    :param str table_name: Name of table
    :param str logger_name: Name of logger
    """
    logger = logging.getLogger(logger_name)

    logger.debug("{}: Pre-processing...".format(table_name))
    # Blank data frame
    data = pd.DataFrame()
    # Open Excel
    for i, file in enumerate(table_data["path"]):
        xls = pd.ExcelFile(file)
        xls_sheets = xls.sheet_names

        if table_data["sheet"][i] is None:
            # Iterate through all sheets if no specific sheet selected
            # PANDAS BUG (pandas = 0.23.0):
            #    For some reason pandas fails to read all sheets if sheet_name=None
            for sheet in xls_sheets:
                df = pd.read_excel(file,
                                   sheet_name=sheet,
                                   index_col=None,
                                   skiprows=table_data["skip_row"][i],
                                   dtype=str,
                                   header=None)

                data = data.append(df, ignore_index=True)
                # Remove unnecessary columns just in case
                data = data.iloc[:, 0:len(table_data["structure"])]

        else:
            # Get only from selected sheet
            df = pd.read_excel(file,
                               sheet_name=table_data['sheet'][i],
                               index_col=None,
                               skiprows=table_data["skip_row"][i],
                               dtype=str,
                               header=None)

            data = data.append(df, ignore_index=True)

    data.columns = table_data["structure"]
    # Remove NaN rows and everything below
    data = data.replace(['nan', 'None'], '', regex=True)  # replaces 'nan' strings with blanks
    rows_b_trunc = len(data)
    try:
        # locate first instance of blank cell in index_col and truncate data up to that cell
        data = data.loc[:data[(data[table_data["index_col"]] == '')].index[0] - 1, :]
        if len(data) == 0:
            raise IndexError
        logger.debug('{}: Trimmed from {} to {} rows'.format(table_name, rows_b_trunc, len(data)))
    except IndexError:
        pass
    logger.debug('{}: Pre-processing complete, {} rows'.format(table_name, len(data)))
    return data


if __name__ == '__main__':
    create_or_update()


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


def save_job_model(data: dict, output_path: str, file_format: str = 'json'):
    """
    Save your job model (parameters) into file.

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


def read_job_model(source: str, file_format: str = 'auto'):
    """
    Read job model (parameters) from existing file.

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
