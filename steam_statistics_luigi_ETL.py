from os import path, remove
from datetime import date
import json

from requests import get
from luigi import run, Task, LocalTarget, DateParameter, Parameter
from pandas import DataFrame  # Do not delete! (pipeline use DataFrame type between functions)

from Steam_statistics_tasks.Logging_Config import logging_config
from Steam_statistics_tasks.Universal_steam_statistics_luigi_task import my_beautiful_task_universal_parser_part,\
    my_beautiful_task_data_frame_merge, my_beautiful_task_data_landing
from Steam_statistics_tasks.AllSteamProductsData_steam_statistics_luigi_task import steam_aps_from_web_api_parser,\
    steam_apps_validator
from Steam_statistics_tasks.GetSteamProductsDataInfo_steam_statistics_luigi_task import steam_apps_parser, \
    safe_dlc_data, parsing_steam_data, apps_and_dlc_df_landing
from Steam_statistics_tasks.SteamAppsInfo_steam_statistics_luigi_task import get_csv_for_join,\
    steam_apps_data_cleaning
"""
Steam statistics Luigi ETL.
"""


class AllSteamProductsData(Task):
    """
    Gets a list of products from the SteamAPI.
    """
    task_namespace = 'AllSteamProductsData'
    priority = 200
    all_steam_products_data_path = Parameter(significant=True, description='Root path for gets all products from steam')
    date_path_part = DateParameter(default=date.today(), description='Date for root path')

    def output(self):
        return LocalTarget(
            path.join(f"{self.all_steam_products_data_path}/{self.date_path_part:%Y/%m/%d}/{'_Validate_Success'}"))

    def run(self):
        if path.exists(f"{self.all_steam_products_data_path}"
                       f"{self.date_path_part:%Y/%m/%d}/{'_Validate_Success'}") is False:
            steam_api_response = get('http://api.steampowered.com/ISteamApps/GetAppList/v2')
            steam_apps_list = json.loads(steam_api_response.text)
            steam_apps_list = steam_aps_from_web_api_parser(steam_apps_list)
            partition_path = f"{self.all_steam_products_data_path}"
            steam_apps_list = steam_apps_validator(steam_apps_list, partition_path)
            day_for_landing = f"{self.date_path_part:%Y/%m/%d}"
            my_beautiful_task_data_landing(steam_apps_list, day_for_landing,
                                           partition_path, "AllSteamProductsData.json")


class GetSteamProductsDataInfo(Task):
    """
    Parses and scrapes the list of products available on Steam.
    """
    task_namespace = 'GetSteamProductsDataInfo'
    priority = 5000
    get_steam_products_data_info_path = Parameter(significant=True,
                                                  description='Root path for gets info about steam products')
    date_path_part = DateParameter(default=date.today(), description='Date for root path')
    get_steam_products_data_info_logfile_path = Parameter(default="steam_products_data_info.log",
                                                          description='Path for ".log" file')
    get_steam_products_data_info_loglevel = Parameter(default=30, description='Log Level')

    def requires(self):
        return {'AllSteamProductsData': AllSteamProductsData()}

    def output(self):
        return LocalTarget(
            path.join(
                f"{self.get_steam_products_data_info_path}/{'Apps_info'}/{self.date_path_part:%Y/%m/%d}/{'_Validate_Success'}"))

    def run(self):
        logging_config(self.get_steam_products_data_info_logfile_path, int(self.get_steam_products_data_info_loglevel))
        result_successor = self.input()['AllSteamProductsData']
        interested_data = my_beautiful_task_universal_parser_part(result_successor, ".json", drop_list=None)
        interested_data = steam_apps_parser(interested_data)
        apps_df = None
        dlc_df = safe_dlc_data(self.get_steam_products_data_info_path)
        day_for_landing = f"{self.date_path_part:%Y/%m/%d}"
        apps_and_dlc_df_list = parsing_steam_data(interested_data, self.get_steam_products_data_info_path,
                                                  day_for_landing, apps_df, dlc_df)
        apps_df, dlc_df = apps_and_dlc_df_list[0], apps_and_dlc_df_list[1]
        apps_df_save_path = f"{self.get_steam_products_data_info_path}/{'Apps_info'}"
        dlc_df_save_path = f"{self.get_steam_products_data_info_path}/{'DLC_info'}"
        apps_and_dlc_df_landing(apps_df, dlc_df, day_for_landing, apps_df_save_path, dlc_df_save_path)
        safe_dict_data_path = f"{self.get_steam_products_data_info_path}/{'Apps_info'}/" \
                              f"{day_for_landing}/{'_safe_dict_data'}"
        if path.isfile(safe_dict_data_path):
            remove(safe_dict_data_path)
        safe_dict_dlc_data_path = f"{self.get_steam_products_data_info_path}/{'DLC_info'}/" \
                                  f"{day_for_landing}/{'_safe_dict_dlc_data'}"
        if path.isfile(safe_dict_dlc_data_path):
            remove(safe_dict_dlc_data_path)


class SteamAppsInfo(Task):
    """
    Merges all raw CSV tables into one MasterData.
    '''
    Объединяет все сырые CSV таблицы в одну MasterData.
    """
    task_namespace = 'SteamAppsInfo'
    priority = 100
    steam_apps_info_path = Parameter(significant=True, description='Path to join all GetSteamProductsDataInfo .csv')
    date_path_part = DateParameter(default=date.today(), description='Date for root path')

    def requires(self):
        return {'GetSteamProductsDataInfo': GetSteamProductsDataInfo()}

    def output(self):
        return LocalTarget(
            path.join(f"{self.steam_apps_info_path}/{self.date_path_part:%Y/%m/%d}/{'_Validate_Success'}"))

    def run(self):
        result_successor = self.input()['GetSteamProductsDataInfo']
        interested_data = get_csv_for_join(result_successor)
        all_apps_data_frame = None
        for data in interested_data.values():
            all_apps_data_frame = my_beautiful_task_data_frame_merge(all_apps_data_frame, data)
        all_apps_data_frame = steam_apps_data_cleaning(all_apps_data_frame)
        day_for_landing = f"{self.date_path_part:%Y/%m/%d}"
        my_beautiful_task_data_landing(all_apps_data_frame, day_for_landing,
                                       self.steam_apps_info_path, "SteamAppsInfo.csv")


if __name__ == "__main__":
    run()
