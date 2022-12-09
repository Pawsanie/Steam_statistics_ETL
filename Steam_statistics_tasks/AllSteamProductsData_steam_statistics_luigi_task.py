import json
from os import walk, path
from datetime import date

from requests import get, Response
from pandas import DataFrame
from luigi import Parameter, DateParameter

from .Universal_steam_statistics_luigi_task import UniversalLuigiTask
"""
Contains code for luigi task 'AllSteamAppsData'.
"""


class AllSteamProductsData(UniversalLuigiTask):
    """
    Gets a list of products from the SteamAPI.
    """
    # Luigi parameters:
    landing_path_part = Parameter(significant=True, description='Root path for landing task result.')
    file_mask = Parameter(significant=True, description='File format for landing.')
    file_name = Parameter(significant=True, description='File name for landing.')
    date_path_part = DateParameter(default=date.today(), description='Date for root path')
    # Task settings:
    task_namespace = 'AllSteamProductsData'
    priority = 300

    def run(self):
        self.date_path_part: str = f"{self.date_path_part:%Y/%m/%d}"
        self.output_path: str = path.join(*[str(self.landing_path_part), self.date_path_part])
        if path.exists(path.join(*[
            str(self.landing_path_part),
            self.date_path_part,
            self.success_flag])
                       ) is False:
            steam_api_response: Response = get('http://api.steampowered.com/ISteamApps/GetAppList/v2')
            steam_apps_list: dict[str] = json.loads(steam_api_response.text)
            steam_apps_list: dict[str] = self.steam_aps_from_web_api_parser(steam_apps_list)
            partition_path: str = f"{self.landing_path_part}"
            steam_apps_list: DataFrame = self.steam_apps_validator(steam_apps_list, partition_path)
            self.task_data_landing(data_to_landing=steam_apps_list)

    def steam_aps_from_web_api_parser(self, interested_data: dict[str]) -> dict[str]:
        """
        Parses the result received from the Steam Web-API.
        """
        all_aps_data: dict[str] = interested_data
        all_aps_data: dict[str] = all_aps_data \
            .get('applist') \
            .get('apps')
        return all_aps_data

    def steam_apps_validator(self, steam_apps_list: dict[str], partition_path: str) -> DataFrame:
        """
        If the result of work 'AllSteamAppsData' already exists, checks it for duplicates
        and saves in the last iteration only new product available on Steam.
        """
        file_list: list = []
        for dirs, folders, files in walk(partition_path):
            for file in files:
                if self.success_flag in file:
                    path_to_file: str = path.join(*[dirs, file])
                    file_list.append(path_to_file)
        if len(file_list) != 0:
            self.result_successor = file_list
            interested_data: dict[str, DataFrame] = self.task_universal_parser_part()
            all_apps_parsing_data: None = None
            for data in interested_data.values():
                all_apps_parsing_data: DataFrame = self.data_frames_merge(
                    data_from_files=all_apps_parsing_data,
                    extracted_data=data)
            new_steam_apps_list: DataFrame = DataFrame(steam_apps_list)

            interested_apps: DataFrame = new_steam_apps_list[
                ~new_steam_apps_list['name']
                .isin(all_apps_parsing_data['name'])]\
                .reset_index(drop=True)
        else:
            interested_apps: dict[str] = steam_apps_list
        return interested_apps
