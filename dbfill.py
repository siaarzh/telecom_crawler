import cx_Oracle
from configparser import ConfigParser
import logging
import os


class DB:
    """Pseudo-class for Oracle connections"""

    def __init__(self, settings):
        """
        Basic logging set-up, configuration file parsing, connection test
        """
        # Logging set-up
        self._application_name = "DbFill"
        self._logger = logging.getLogger(self._application_name)
        self._logger.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')

        file_handler = logging.FileHandler('logs/dbfill.log')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)

        console = logging.StreamHandler()
        console.setLevel(logging.WARNING)
        console.setFormatter(formatter)

        self._logger.addHandler(console)
        self._logger.addHandler(file_handler)

        try:
            # Parse configuration from INI
            parser = ConfigParser()
            parser.read(settings)
            _conn_oracle_sett = {}
            if parser.has_section('oracle'):
                params = parser.items('oracle')
                for param in params:
                    _conn_oracle_sett[param[0]] = param[1]
            else:
                raise Exception('{} is missing section [oracle]'.format(settings))
            self._conn_oracle_sett = _conn_oracle_sett

            # Connection test
            self._dsn = cx_Oracle.makedsn(self._conn_oracle_sett["host"], self._conn_oracle_sett["port"],
                                          self._conn_oracle_sett["sid"])

            self._oracle_conn = cx_Oracle.connect(self._conn_oracle_sett["user"], self._conn_oracle_sett["password"],
                                                  self._dsn, encoding="UTF-8", nencoding="UTF-16")

        except Exception as e:
            self._logger.exception(e)


class DbFill(DB):
    """
    Oracle database-fill class with capability to encode Kazakh letters for error-free transfer
    to database.
    """
    def __init__(self, settings):
        super(DbFill, self).__init__(settings)

    def _get_data_encode(self, data, charset):
        result = []
        for dic in data:
            tmp_dic = {}
            for key, val in dic.items():
                tmp_dic[key] = val.encode(charset, 'replace').decode(charset)
            result.append(tmp_dic)
        return result
        
    def _kaz_encode(self, data):
        '''
        Encode all Kazakh letters that do not get decoded by the ISO8859-5 codec
        into unicode format of the form:
            \XXXX
        '''

        result = []
        bad_symbols = {}
        for entry in data:
            tmp_entry = {}
            for key, word in entry.items():
                try:
                    tmp_word = word.replace('\\','//')
                    tmp_word = tmp_word.replace('nan','')
                    for s in word:
                        try:
                            s.encode('iso8859-5')
                        except UnicodeEncodeError:
                            # print('found it: {}'.format(s))
                            # print('replacing with: {}'.format(s.encode('unicode-escape').decode('utf-8').replace(r'\u0', r'\0')))
                            # input("Press Enter .................................")
                            tmp_word = tmp_word.replace(s, s.encode('unicode-escape').decode('utf-8'))
                            # Replace \u with \ for oracle and expand \x into \00
                            tmp_word = tmp_word.replace('\\u', '\\')
                            tmp_word = tmp_word.replace('\\x', '\\00')
                            
                            bad_symbols[s] = s.encode('unicode-escape').decode('utf-8').replace(r'\u0', r'\0')
                    tmp_entry[key] = tmp_word
                except AttributeError as e:
                    self._logger.exception(e)
            result.append(tmp_entry)
        
        return result

    def fill_main_storage(self, table_name, structure, data, charset="utf-8", nvar_cols=None):
        """Fill table"""
        try:
            data = self._kaz_encode(data)
            
            sql = "insert into {} (".format(table_name)
            param_vals_lst = []
            # Encase unicode string literals with UNISTR() for columns with keyword in column name
            if nvar_cols:
                keywords = nvar_cols
            else:
                keywords = ["kaz", "kz", "name", "address", "activity", "fio"]
            for i, head in enumerate(structure):
                if any(kw in head.lower() for kw in keywords):
                    param_vals_lst.append("UNISTR(:{})".format(head))
                else:
                    param_vals_lst.append(":{}".format(head))

            columns_statement = ', '.join(structure)
            values_statement = ', '.join(param_vals_lst)
            sql = sql + columns_statement + ') values (' + values_statement + ')'
            
            or_cur = self._oracle_conn.cursor()
            or_cur.prepare(sql)
            or_cur.executemany(None, data)
            self._oracle_conn.commit()
        except Exception as e:
            self._logger.exception(e)
        

    def send_command(self, command):
        or_cur = self._oracle_conn.cursor()
        try:
            if isinstance(command, list):
                for line in command:
                    or_cur.execute(line)
            elif isinstance(command, str):
                or_cur.execute(command)
        except Exception as e:
            self._logger.exception(e)

    def get_num_rows(self, table_name):
        or_cur = self._oracle_conn.cursor()
        or_cur.execute("SELECT COUNT(*) from {}".format(table_name))
        result = or_cur.fetchone()
        return result[0]
