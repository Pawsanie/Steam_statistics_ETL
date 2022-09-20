from os import path
from datetime import date
import json

from requests import get
from luigi import run, Task, LocalTarget, DateParameter, Parameter
from pandas import DataFrame

from Steam_statistics_tasks.Universal_steam_statistics_luigi_task import my_beautiful_task_data_landing
from Steam_statistics_tasks.AllSteamProductsData_steam_statistics_luigi_task import steam_aps_from_web_api_parser, \
    steam_apps_validator
from Steam_statistics_tasks.GetSteamProductsDataInfo_steam_statistics_luigi_task import \
    get_steam_products_data_info_steam_statistics_luigi_task_run
from Steam_statistics_tasks.SteamProductsInfoCSVJoiner_universal_steam_statistics_luigi_task import \
    steam_products_info_run

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
            steam_apps_list: dict[str] = steam_aps_from_web_api_parser(steam_apps_list)
            partition_path = f"{self.all_steam_products_data_path}"
            steam_apps_list: DataFrame = steam_apps_validator(steam_apps_list, partition_path)
            day_for_landing = f"{self.date_path_part:%Y/%m/%d}"
            my_beautiful_task_data_landing(steam_apps_list, f"{partition_path}/{day_for_landing}",
                                           "AllSteamProductsData.json")


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
                f"{self.get_steam_products_data_info_path}/{self.date_path_part:%Y/%m/%d}/{'_Validate_Success'}"))

    def run(self):
        get_steam_products_data_info_steam_statistics_luigi_task_run(self)


class SteamAppInfoCSVJoiner(Task):
    """
    Merges all raw CSV tables into one MasterData.
    """
    task_namespace = 'SteamAppsInfo'
    priority = 100
    steam_apps_info_path = Parameter(significant=True, description='Path to join all GetSteamProductsDataInfo .csv')
    date_path_part = DateParameter(default=date.today(), description='Date for root path')

    directory_for_csv_join = 'Apps_info'
    csv_file_for_result = "SteamAppsInfo.csv"

    def requires(self):
        return {'GetSteamProductsDataInfo': GetSteamProductsDataInfo()}

    def output(self):
        return LocalTarget(
            path.join(f"{self.steam_apps_info_path}/{self.date_path_part:%Y/%m/%d}/{'_Validate_Success'}"))

    def run(self):
        steam_products_info_run(self)


if __name__ == "__main__":
    run()
