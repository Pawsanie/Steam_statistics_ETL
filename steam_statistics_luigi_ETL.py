from luigi import run, Task, LocalTarget, DateParameter, Parameter
from os import path, remove
from pandas import DataFrame  # Do not delete! (pipeline use DataFrame type between functions)
from datetime import date
from requests import get
import json
from Universal_steam_statistics_luigi_task import my_beautiful_task_universal_parser_part,\
    my_beautiful_task_data_frame_merge, my_beautiful_task_data_landing
from AllSteamAppsData_steam_statistics_luigi_task import steam_aps_from_web_api_parser
from GetSteamAppInfo_steam_statistics_luigi_task import steam_apps_parser, safe_dlc_data, \
    parsing_steam_data, apps_and_dlc_df_landing
from AppInfoCSVJoiner_steam_statistics_luigi_task import get_csv_for_join, steam_apps_data_cleaning


class AllSteamAppsData(Task):
    """
    Gets a list of applications from the SteamAPI.
    '''
    Получает список приложений от SteamAPI.
    """
    task_namespace = 'AllSteamAppsData'
    priority = 200
    all_steam_apps_path = Parameter(significant=True, description='Root path for gets all aps from steam')
    date_path_part = DateParameter(default=date.today(), description='Date for root path')

    def output(self):
        return LocalTarget(
            path.join(f"{self.all_steam_apps_path}/{self.date_path_part:%Y/%m/%d}/{'_Validate_Success'}"))

    def run(self):
        if path.exists(f"{self.all_steam_apps_path}{self.date_path_part:%Y/%m/%d}/{'_Validate_Success'}") is False:
            steam_api_response = get('http://api.steampowered.com/ISteamApps/GetAppList/v2')
            steam_apps_list = json.loads(steam_api_response.text)
            steam_apps_list = steam_aps_from_web_api_parser(steam_apps_list)
            partition_path = f"{self.all_steam_apps_path}"
            day_for_landing = f"{self.date_path_part:%Y/%m/%d}"
            my_beautiful_task_data_landing(steam_apps_list, day_for_landing,
                                           partition_path, "All_Steam_Apps_Data.json")


class GetSteamAppInfo(Task):
    """
    Parses and scrapes the list of apps available on Steam.
    '''
    Парсит и скрапит список приложений доступных в Steam.
    """
    task_namespace = 'GetSteamAppInfo'
    priority = 5000
    get_steam_app_info_path = Parameter(significant=True, description='Root path for gets info about steam apps')
    date_path_part = DateParameter(default=date.today(), description='Date for root path')

    def requires(self):
        return {'AllSteamAppsData': AllSteamAppsData()}

    def output(self):
        return LocalTarget(
            path.join(
                f"{self.get_steam_app_info_path}/{'Apps_info'}/{self.date_path_part:%Y/%m/%d}/{'_Validate_Success'}"))

    def run(self):
        result_successor = self.input()['AllSteamAppsData']
        interested_data = my_beautiful_task_universal_parser_part(result_successor, ".json", drop_list=None)
        interested_data = steam_apps_parser(interested_data)
        apps_df = None
        dlc_df = safe_dlc_data(self.get_steam_app_info_path)
        day_for_landing = f"{self.date_path_part:%Y/%m/%d}"
        apps_and_dlc_df_list = parsing_steam_data(interested_data, self.get_steam_app_info_path,
                                                  day_for_landing, apps_df, dlc_df)
        apps_df, dlc_df = apps_and_dlc_df_list[0], apps_and_dlc_df_list[1]
        apps_df_save_path = f"{self.get_steam_app_info_path}/{'Apps_info'}"
        dlc_df_save_path = f"{self.get_steam_app_info_path}/{'DLC_info'}"
        apps_and_dlc_df_landing(apps_df, dlc_df, day_for_landing, apps_df_save_path, dlc_df_save_path)
        safe_dict_data_path = f"{self.get_steam_app_info_path}/{'Apps_info'}/{day_for_landing}/{'_safe_dict_data'}"
        if path.isfile(safe_dict_data_path):
            remove(safe_dict_data_path)
        safe_dict_dlc_data_path = f"{self.get_steam_app_info_path}/{'DLC_info'}/" \
                                  f"{day_for_landing}/{'_safe_dict_dlc_data'}"
        if path.isfile(safe_dict_dlc_data_path):
            remove(safe_dict_dlc_data_path)


class AppInfoCSVJoiner(Task):
    """
    Merges all raw CSV tables into one masterdata.
    '''
    Объединяет все сырые CSV таблицы в одну masterdata.
    """
    task_namespace = 'AppInfoCSVJoiner'
    priority = 100
    app_info_csv_joiner_path = Parameter(significant=True, description='Path to join all GetSteamAppInfo.csv')
    date_path_part = DateParameter(default=date.today(), description='Date for root path')

    def requires(self):
        return {'GetSteamAppInfo': GetSteamAppInfo()}

    def output(self):
        return LocalTarget(
            path.join(f"{self.app_info_csv_joiner_path}/{self.date_path_part:%Y/%m/%d}/{'_Validate_Success'}"))

    def run(self):
        result_successor = self.input()['GetSteamAppInfo']
        interested_data = get_csv_for_join(result_successor)
        all_apps_data_frame = None
        for data in interested_data.values():
            all_apps_data_frame = my_beautiful_task_data_frame_merge(all_apps_data_frame, data)
        all_apps_data_frame = steam_apps_data_cleaning(all_apps_data_frame)
        day_for_landing = f"{self.date_path_part:%Y/%m/%d}"
        my_beautiful_task_data_landing(all_apps_data_frame, day_for_landing,
                                       self.app_info_csv_joiner_path, "All_Steam_App_Info.csv")


if __name__ == "__main__":
    run()
