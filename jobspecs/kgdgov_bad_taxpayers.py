#!/usr/bin/env python3
import os


def create(store=None):
    """
    List all companies and persons with bad tax records and violations from kgd.gov.kz

    :param store: full path to storage of temporary files, if store==None
                  then uses default data/ directory
    :return: store model
    :rtype: dict
    """

    root_path = os.path.dirname(os.path.dirname(__file__))
    if not store:
        store = os.path.join(root_path, 'data')

    # 1. Define tables
    table_names = ["CR_KGDGOV_PSEUDO_COMPANY",
                   "CR_KGDGOV_WRONG_ADDRESS",
                   "CR_KGDGOV_BANKRUPT",
                   "CR_KGDGOV_INACTIVE",
                   "CR_KGDGOV_INVALID_REGISTRATION",
                   "CR_KGDGOV_VIOLATION_TAX_CODE",
                   "CR_KGDGOV_TAX_ARREARS_150",  # MRP > 150
                   "CR_KGDGOV_TAX_ARREARS_10"]  # MRP > 10

    structures = [
        ["Num", "BIN", "RNN", "taxpayer_organization", "taxpayer_name", "owner_name", "owner_IIN", "owner_RNN",
         "court_decision", "illegal_activity_start_date"],
        ["Num", "BIN", "RNN", "taxpayer_organization", "taxpayer_name", "owner_name", "owner_IIN", "owner_RNN",
         "inspection_act_no", "inspection_date"],
        ["Num", "BIN", "RNN", "taxpayer_organization", "taxpayer_name", "owner_name", "owner_IIN", "owner_RNN",
         "court_decision", "court_decision_date"],
        ["Num", "BIN", "RNN", "taxpayer_organization", "taxpayer_name", "owner_name", "owner_IIN", "owner_RNN",
         "order_no", "order_date"],
        ["Num", "BIN", "RNN", "taxpayer_organization", "taxpayer_name", "owner_name", "owner_IIN", "owner_RNN",
         "court_decision_no", "court_decision_date"],
        ["Num", "BIN", "RNN", "taxpayer_organization", "owner_name", "owner_IIN", "owner_RNN",
         "order_no", "order_date", "violation_type"],
        ["Num", "region", "office_of_tax_enforcement", "OTE_ID", "BIN", "RNN", "taxpayer_organization_ru",
         "taxpayer_organization_kz", "last_name_kz", "first_name_kz", "middle_name_kz", "last_name_ru", "first_name_ru",
         "middle_name_ru", "owner_IIN", "owner_RNN", "owner_name_kz", "owner_name_ru", "economic_sector", "total_due",
         "sub_total_main", "sub_total_late_fee", "sub_total_fine"],
        ["Num", "region", "office_of_tax_enforcement", "OTE_ID", "BIN", "RNN", "taxpayer_organization_ru",
         "taxpayer_organization_kz", "last_name_kz", "first_name_kz", "middle_name_kz", "last_name_ru", "first_name_ru",
         "middle_name_ru", "owner_IIN", "owner_RNN", "owner_name_kz", "owner_name_ru", "economic_sector", "total_due",
         "sub_total_main", "sub_total_late_fee", "sub_total_fine"]
    ]

    index_cols = ['Num'] * 8

    # 2. Define urls
    root_url = "http://kgd.gov.kz/mobile_api/services/taxpayers_unreliable_exportexcel"
    sub_urls = ["/PSEUDO_COMPANY/KZ_ALL/fileName/list_PSEUDO_COMPANY_KZ_ALL.xlsx",
                # Список налогоплательщиков, признанных лжепредприятиями
                "/WRONG_ADDRESS/KZ_ALL/fileName/list_WRONG_ADDRESS_KZ_ALL.xlsx",
                # Список налогоплательщиков, отсутствующих по юридическому адресу
                "/BANKRUPT/KZ_ALL/fileName/list_BANKRUPT_KZ_ALL.xlsx",
                # Список налогоплательщиков, признанных банкротами
                "/INACTIVE/KZ_ALL/fileName/list_INACTIVE_KZ_ALL.xlsx",
                # Список налогоплательщиков, признанных бездействующими
                "/INVALID_REGISTRATION/KZ_ALL/fileName/list_INVALID_REGISTRATION_KZ_ALL.xlsx",
                # Список налогоплательщиков, регистрация которых признана недействительной
                "/VIOLATION_TAX_CODE/KZ_ALL/fileName/list_VIOLATION_TAX_CODE_KZ_ALL.xlsx",
                # Список налогоплательщиков, реорганизованных с нарушением норм Налогового кодекса
                "/TAX_ARREARS_150/KZ_ALL/fileName/list_TAX_ARREARS_150_KZ_ALL.xlsx",
                # 1 Список налогоплательщиков, имеющих налоговую задолженность
                "/TAX_ARREARS_150/KZ_ALL/fileName/list_TAX_ARREARS_150_KZ_ALL.xlsx"
                # 2 Список налогоплательщиков, имеющих налоговую задолженность
                ]
    urls = [[root_url + sub_url] for sub_url in sub_urls]

    # 3. Define filters
    sheets    = [[None]] * 6
    sheets.extend([[0, ], [1, ]])
    skip_rows = [[3], [3], [3], [3], [3], [3], [6], [6]]
    last_rows = [[None]] * 8

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