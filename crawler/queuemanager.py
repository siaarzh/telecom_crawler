import importlib
import logging
import os
import pandas as pd
import re
import zipfile
from urllib import parse

import rarfile
import requests

from crawler.utils import save_sources_config, read_sources_config


def create_or_update():
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
    if len(os.listdir('jobspecs')) == 0:
        return {'success': False,
                'message': 'No jobs specified.'}

    os.makedirs('jobs')

    try:
        for job_name in os.listdir('jobspecs'):
            if job_name.endswith(".py") and '__init__' not in job_name:
                job_name = os.path.splitext(job_name)[0]
                job = importlib.import_module("jobspecs.{}".format(job_name))
                source_model = job.create()
                save_sources_config(source_model, os.path.join('jobs', job_name + ".ini"), 'ini')
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
                job = read_sources_config(os.path.join(job_dir, job_name))
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
            logger.debug('{}: downloading {}'.format(table, file_name))
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'wb') as f:
                f.write(result.content)

            _, file_ext = os.path.splitext(file_path)

            # 3. (Extract and) Append path to spreadsheet
            if file_ext in (".xls", ".xlsx"):

                logger.info('{}: saving {}'.format(table, file_name))
                job_queue[table]["path"].append(file_path)
                temp_sheet.append(job_queue[table]["sheet"][i])
                temp_skip_row.append(job_queue[table]["skip_row"][i])

            elif file_ext in ['rar', 'zip']:
                if file_ext == '.rar':
                    logger.debug("{}: checking contents of {}".format(table, file_name))
                    archive = rarfile.RarFile(file_path)
                elif file_ext == '.zip':
                    logger.debug("{}: checking contents of {}".format(table, file_name))
                    archive = zipfile.ZipFile(file_path)
                else:
                    logger.error("{}: Could not recognize archive format. \n"
                                 "Can only work with rar or zip")
                    break
                for idx, f in enumerate(archive.namelist()):
                    # check for excel and extract
                    _, f_ext = os.path.splitext(f)
                    if f_ext in (".xls", ".xlsx"):
                        archive.extract(f, table_info["store"])
                        logger.info("{}: saving {} (from {})".format(table, f, file_name))
                        job_queue[table]["path"].append(os.path.join(table_info["store"], f))
                        temp_sheet.append(job_queue[table]["sheet"][i])
                        temp_skip_row.append(job_queue[table]["skip_row"][i])

                logger.debug("{}: removing {}".format(table, file_name))
                os.remove(file_path)

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

    logger.info("{}: Pre-processing...".format(table_name))
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
        logger.warning('{}: truncated from {} to {} rows'.format(table_name, rows_b_trunc, len(data)))
    except IndexError:
        pass
    logger.info('{}: Pre-processing complete, {} rows'.format(table_name, len(data)))
    return data


if __name__ == '__main__':
    create_or_update()
