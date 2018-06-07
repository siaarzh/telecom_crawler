#!/usr/bin/env python3
import os
import re

import requests
from bs4 import BeautifulSoup


def create(store=None):
    """
    Download list of all taxpaying companies from stat.gov.kz

    :param store: full path to storage of temporary files, if store==None
                  then uses default data/ directory
    :return: source model
    :rtype: dict
    """

    root_path = os.path.dirname(os.path.dirname(__file__))
    if not store:
        store = os.path.join(root_path, 'data')

    # 1. Define tables
    table_names = ["CR_STATGOV_COMPANIES"]

    structures = [["BIN",
                   "Full_Name_Kz",
                   "Full_Name_Ru",
                   "Registration_Date",
                   "OKED_1",
                   "Activity_Kz",
                   "Activity_Ru",
                   "OKED_2",
                   "KRP",
                   "KRP_Name_Kz",
                   "KRP_Name_Ru",
                   "KATO",
                   "Settlement_Kz",
                   "Settlement_Ru",
                   "Legal_address",
                   "Head_FIO"]
                  ]

    index_cols = ["Full_Name_Ru"]

    # 2. Define urls
    root_url = "http://stat.gov.kz"


    def list_urls_statgov_companies():
        # get list of urls from statgov businesses registry
        registry_url = "http://stat.gov.kz/faces/publicationsPage/publicationsOper/homeNumbersBusinessRegisters/homeNumbersBusinessRegistersReestr"
        html = requests.get(registry_url).text
        soup = BeautifulSoup(html, 'lxml')
        urls = soup.find("div", attrs={"id": "pt1:r1:0:j_id__ctru0pc3:pgl4"}).find_all("a", href=re.compile("ESTAT"))
        urls = [url.get('href') for url in urls]

        return urls


    urls = [[root_url + sub_url for sub_url in list_urls_statgov_companies()]]

    # 3. Define filters
    sheets    = [[None] * len(urls[0]), ]
    skip_rows = [[4] * len(urls[0]), ]
    last_rows = [[None] * len(urls[0]), ]

    # 4. Aggregate
    job_model = {tname: {} for tname in table_names}

    for i, name in enumerate(table_names):
        job_model[table_names[i]] = {"structure": structures[i],
                                                  "index_col": index_cols[i],
                                                  "urls": urls[i],
                                                  "sheet": sheets[i],
                                                  "skip_row": skip_rows[i],
                                                  "last_row": last_rows[i],
                                                  "path": [],
                                                  "store": os.path.join(store, name)
                                                  }

    return job_model

if __name__ == '__main__':
    from utils import print_hierarchy
    print_hierarchy(create())