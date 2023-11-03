from os import path, remove
from ast import literal_eval

from pandas import DataFrame
import logging
"""
Contains the LocalCashReader code.
"""


class LocalCashReader:
    """
    Reads the local cache.
    """
    @staticmethod
    def data_from_file_to_pd_dataframe(safe_dict_data_path: str) -> DataFrame:
        """
        Reads the local cache.
        """
        if path.isfile(safe_dict_data_path):
            with open(safe_dict_data_path, 'r') as safe_dict_data_file:
                rows = safe_dict_data_file.readlines()

            rows_len: int = len(rows) - 1
            if rows_len > 0:
                rows.pop(rows_len)

                with open(safe_dict_data_path, 'w') as safe_dict_data_file:
                    for row in rows:
                        safe_dict_data_file.write(row)
                logging.info('Start merge local_cash...')

                with open(safe_dict_data_path, 'r') as safe_dict_data_file:
                    data: str = safe_dict_data_file.read()
                    apps_df_redy: DataFrame = DataFrame.from_dict(literal_eval(data.replace('\n', ',')))
                    apps_df_redy.reset_index(drop=True)
                    logging.info(str(apps_df_redy) + '\nLocal_cash successfully merged...')

            else:
                remove(safe_dict_data_path)
                apps_df_redy: DataFrame = DataFrame({'app_name': []})

        else:
            apps_df_redy: DataFrame = DataFrame({'app_name': []})

        return apps_df_redy
