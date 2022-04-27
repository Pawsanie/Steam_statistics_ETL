from typing import Optional, Any

from luigi import run, Task, LocalTarget, ExternalTask, ListParameter, DateParameter, Parameter, DictParameter
from os import walk, path
from pandas import DataFrame
from datetime import date, datetime
from requests import get
import json
from steam_statistics_luigi_tasks import my_beautiful_task_data_landing, my_beautiful_task_universal_parser_part, \
    ask_app_in_steam_store


class AllSteamAppsData(Task):
    """
    Получаем список приложений от SteamAPI
    """
    task_namespace = 'AllSteamAppsData'
    priority = 200
    all_steam_apps_path = Parameter(significant=True, description='Root path for gets all aps from steam')
    date_path_part = DateParameter(default=date.today(), description='Dat for root path')

    def output(self):
        return LocalTarget(
            path.join(f"{self.all_steam_apps_path}/{self.date_path_part:%Y/%m/%d}/{'_Validate_Success'}"))

    def run(self):
        if path.exists(f"{self.all_steam_apps_path}{self.date_path_part:%Y/%m/%d}/{'_Validate_Success'}") is False:
            steam_api_response = get('http://api.steampowered.com/ISteamApps/GetAppList/v2')
            steam_apps_list = json.loads(steam_api_response.text)
            # price = get('http://api.steampowered.com/ISteamEconomy/GetAssetPrices/v0001/550')
            partition_path = f"{self.all_steam_apps_path}"
            day_for_landing = f"{self.date_path_part:%Y/%m/%d}"
            my_beautiful_task_data_landing(steam_apps_list, day_for_landing,
                                           partition_path, "AllSteamAppsData.json")


class GetSteamAppInfo(Task):
    """
    Парсим список приложений доступных в Steam
    """
    task_namespace = 'GetSteamAppInfo'
    priority = 500
    get_steam_app_info_path = Parameter(significant=True, description='Root path for gets info about steam apps')
    date_path_part = DateParameter(default=date.today(), description='Dat for root path')

    def requires(self):
        return {'AllSteamAppsData': AllSteamAppsData()}

    def output(self):
        return LocalTarget(path.join(f"{self.get_steam_app_info_path}/"
                                     f"{self.date_path_part:%Y/%m/%d}/{'_Validate_Success'}"))

    def run(self):
        result_successor = self.input()['AllSteamAppsData']
        interested_data = my_beautiful_task_universal_parser_part(result_successor,
                                                                  ".json", drop_list=None)
        for value in interested_data:
            all_aps_data = interested_data.get(value)
            all_aps_data = all_aps_data.iloc[0]
            all_aps_data = all_aps_data.to_dict()
            all_aps_data = all_aps_data.get('applist')
            interested_data = DataFrame(all_aps_data)
            # Удаляем то что не является приложениями:
            interested_data = interested_data[~interested_data['name'].str.contains('Soundtrack')]
            interested_data = interested_data[~interested_data['name'].str.contains('OST')]
            interested_data = interested_data[~interested_data['name'].str.contains('Artbook')]
            interested_data = interested_data[~interested_data['name'].str.contains('Texture')]
            interested_data = interested_data[~interested_data['name'].str.contains('Demo')]
            interested_data = interested_data[~interested_data['name'].str.contains('Playtest')]
            interested_data = interested_data[~interested_data['name'].str.contains('DLC')]
            interested_data = interested_data[~interested_data['name'].str.contains('test2')]
            interested_data = interested_data[~interested_data['name'].str.contains('test3')]
            interested_data = interested_data[~interested_data['name'].str.contains('Pieterw')]
            interested_data = interested_data[~interested_data['name'].str.contains('Closed Beta')]
            interested_data = interested_data[~interested_data['name'].str.contains('Open Beta')]
            interested_data = interested_data[~interested_data['name'].str.contains('RPG Maker')]
            interested_data = interested_data[~interested_data['name'].str.contains('Pack')]
            null_filter = interested_data['name'] != ""
            interested_data = interested_data[null_filter]
            interested_data = interested_data.reset_index()
        # for index in range(len(interested_data)):
        #     app_name = interested_data.iloc[index]['name']
        #     app_id = interested_data.iloc[index]['appid']
            # ask_app_in_steam_store(app_id, app_name)

            result = str(ask_app_in_steam_store('331470', 'Everlasting Summer'))  # test
            print(result)
            partition_path = f"{self.get_steam_app_info_path}"
            day_for_landing = f"{self.date_path_part:%Y/%m/%d}"
            # result_path = f"{partition_path}/{day_for_landing}/{'result_test'}"
            # with open(result_path, 'w') as file:
            #     file.write(result)



if __name__ == "__main__":
    run()
