import os
import sys
import logging
import logging.config
from datetime import timedelta
from time import time

from slackclient import SlackClient

from crawler.dbfill import DbFill
from crawler.queuemanager import queue_jobs, download_extract_files, prepare_data
from crawler.utils import make_log_dir, MsgCounterHandler, internet_on, get_bot_user_token, DummySlackClient


def init_logger():
    global root_path
    global logger
    global slack_client
    global slack_channel

    # set root_path
    root_path = os.path.abspath(os.curdir)

    # set logging config path, you may use the template too
    log_config_file = os.path.join('conf', 'logging.conf')

    # set logging config path, you may use the template too
    slack_config_file = os.path.join('conf', 'slack.conf')

    if os.path.exists(log_config_file):
        # create logger
        make_log_dir(log_config_file)
        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger('crawler')
        counth = MsgCounterHandler()
        counth.setLevel(logging.DEBUG)
        logger.addHandler(counth)

    else:
        # create a backup logger
        logger = logging.getLogger('crawler')
        logger.setLevel(logging.DEBUG)

        # create console handler and set level to debug
        ch = logging.StreamHandler(stream=sys.stdout)
        ch.setLevel(logging.DEBUG)

        # create a messages counter handler
        counth = MsgCounterHandler()
        counth.setLevel(logging.DEBUG)

        # create formatter
        formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')

        # add formatter to ch
        ch.setFormatter(formatter)

        # add ch to logger
        logger.addHandler(ch)
        logger.addHandler(counth)
        logger.warning('Using default console logger')

    if os.path.exists(slack_config_file):
        slack_token, slack_channel = get_bot_user_token(slack_config_file)
        slack_client = SlackClient(slack_token)
        if slack_client.rtm_connect(with_team_state=False):
            logger.debug('Slack bot connected')
        else:
            logger.warning('Slack bot failed to connect!')
    else:
        slack_client = DummySlackClient()
        slack_channel = None
        logger.debug('Dummy Slack bot initialized')


def run():
    init_logger()
    if not internet_on():
        logger.error("No internet connection.")
        return

    t0 = time()

    logger.info("Downloading and Extracting... ")
    job_queue = queue_jobs()  # Get jobs
    job_queue = download_extract_files(job_queue)  # Download, extract, update paths

    db = DbFill(os.path.join('conf', 'database.ini'))

    for table_name, table_data in job_queue.items():
        try:
            db.purge(table_name)
            logger.info('{}: Table cleared!'.format(table_name))

            data = prepare_data(table_data, table_name)
            data_rows = len(data)
            structure = table_data["structure"]

            # conversion to list of dictionaries (see https://stackoverflow.com/a/29815523/8510370)
            data = list(data.T.to_dict().values())
            logger.debug("{}: Storing to database...".format(table_name))
            db.fill_main_storage(table_name, structure, data, "utf-8")

            # check for successful write to database
            logger.debug("{}: Checking row integrity...".format(table_name))
            db_rows = db.get_num_rows(table_name)
            if int(db_rows) == data_rows:
                logger.info("{}: Successfully stored in database!".format(table_name))
            else:
                logger.error("{}: Row count mismatch, something went wrong.".format(table_name))
        except Exception as e:
            logger.error("{}: {}".format(table_name, e))

    log_count = logger.handlers[2].level2count
    end_msg = "`telecom_crawler` task completed\n" + \
              "```" + \
              "\n".join(["{}: {}".format(l, c) for l, c in log_count.items()]) + \
              "```\n" + \
              "time elapsed: {}".format(timedelta(seconds=time() - t0))
    slack_client.api_call(
        "chat.postMessage",
        channel=slack_channel,
        text=end_msg
    )
    print(end_msg.replace('`', ''))
