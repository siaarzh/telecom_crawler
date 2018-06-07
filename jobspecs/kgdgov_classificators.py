#!/usr/bin/env python3
import os


def create(store=None):
    """
    List of classificators from stat.gov.kz

    :param store: full path to storage of temporary files, if store==None
                  then uses default data/ directory
    :return: store model
    :rtype: dict
    """

    root_path = os.path.dirname(os.path.dirname(__file__))
    if not store:
        store = os.path.join(root_path, 'data')

    # 1. Define tables
    table_names = ["CR_STATGOV_OKED",
                   "CR_STATGOV_KPVED",
                   "CR_STATGOV_KATO",
                   "CR_STATGOV_NVED",
                   "CR_STATGOV_KURK",
                   "CR_STATGOV_MKEIS"]

    structures = [["Code", "Name_Kaz", "Name_Rus"],
                  ["Code", "Name_Kaz", "Name_Rus"],
                  ["te", "ab", "cd", "ef", "hij", "k", "name_kaz", "name_rus", "nn"],
                  ["Code", "Name_Kaz", "Name_Rus"],
                  ["Code", "Name_Rus", "Name_Kaz"],
                  ["Code", "Name_Kaz", "Name_Rus"]]

    index_cols = ["Code",
                  "Code",
                  "te",
                  "Code",
                  "Code",
                  "Code"]

    # 2. Define urls
    root_url = "http://stat.gov.kz"
    sub_urls = ["/getImg?id=ESTAT116572",
                # XLSX:  Общий классификатор видов экономической деятельности
                "/getImg?id=ESTAT116569",
                # XLS:   Классификатор продукции по видам экономической деятельности
                "/getImg?id=ESTAT245918",
                # RAR:   Классификатор административно-территориальных объектов
                "/getImg?id=ESTAT181313",
                # XLSX:  Номенклатура видов экономической деятельности
                "/getImg?id=WC16200004875",
                # XLS: Кодификатор улиц Республики Казахстан
                "/getImg?id=ESTAT093569"
                # XLSX:  Межгосударственный классификатор единиц измерения и счета
                ]
    urls = [[root_url + url] for url in sub_urls]

    # 3. Define filters
    sheets    = [[None]] * 7
    skip_rows = [[3], [3], [1], [3], [1], [4]]
    last_rows = [[None]] * 7

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