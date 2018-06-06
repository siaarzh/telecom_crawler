import logging
import os
import re
from urllib import parse

import requests


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

    except NameError as Ne:
        raise Ne
    except Exception as e:
        raise e

    return result, file_name


def download_extract_files(job_queue: dict, logger_name: str = 'test_crawler'):
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

            logger.debug('{}: downloading {}'.format(table, file_name))
            # save file ...

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
                    # open rar archive ...
                elif file_ext == '.zip':
                    logger.debug("{}: checking contents of {}".format(table, file_name))
                    # open zip archive ...
                else:
                    logger.error("{}: Could not recognize archive format. \n"
                                 "Can only work with rar or zip")
                    break
                # extract files ...

                logger.debug("{}: removing {}".format(table, file_name))
                # remove archive ...

        job_queue[table]["sheet"] = temp_sheet
        job_queue[table]["skip_row"] = temp_skip_row

    return job_queue
