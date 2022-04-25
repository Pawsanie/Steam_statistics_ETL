from luigi import run, Task, LocalTarget, ExternalTask, ListParameter, DateParameter, Parameter, DictParameter
from os import walk, path
from pandas import DataFrame
from datetime import date, datetime
from requests import get
import json
from steam_statistics_luigi_tasks import my_beautiful_task_data_landing, my_beautiful_task_universal_parser_part


class AllSteamAppsData(Task):
    task_namespace = 'AllSteamAppsData'
    priority = 200
    all_steam_apps_path = Parameter(significant=True, description='Root path for gets all aps from steam')
    date_path_part = DateParameter(default=date.today(), description='Dat for root path')

    def output(self):
        return LocalTarget(path.join(f"{self.all_steam_apps_path}/{self.date_path_part:%Y/%m/%d}/{'_Validate_Success'}"))

    def run(self):
        if path.exists(f"{self.all_steam_apps_path}{self.date_path_part:%Y/%m/%d}/{'_Validate_Success'}") is False:
            steam_api_response = get('http://api.steampowered.com/ISteamApps/GetAppList/v2')
            steam_apps_list = json.loads(steam_api_response.text)
            #price = get('http://api.steampowered.com/ISteamEconomy/GetAssetPrices/v0001/550')
            partition_path = f"{self.all_steam_apps_path}"
            day_for_landing = f"{self.date_path_part:%Y/%m/%d}"
            flag_path = my_beautiful_task_data_landing(steam_apps_list, day_for_landing,
                                                       partition_path, "AllSteamAppsData.json")
            date_for_path = datetime.strftime(self.date_path_part, "%Y/%m/%d")


class GetSteamAppInfo(Task):
    task_namespace = 'GetSteamAppInfo'
    priority = 500
    get_steam_app_info_path = Parameter(significant=True, description='Root path for gets info about steam apps')
    date_path_part = DateParameter(default=date.today(), description='Dat for root path')

    def requires(self):
        # return AllSteamAppsData()
        return {'AllSteamAppsData': AllSteamAppsData()}

    def output(self):
        return LocalTarget(path.join(f"{self.get_steam_app_info_path}/{self.date_path_part:%Y/%m/%d}/{'_Validate_Success'}"))

    def run(self):
        result_successor = self.input()['AllSteamAppsData']
        print(result_successor)
        interested_data = my_beautiful_task_universal_parser_part(result_successor,
                                                                  "AllSteamAppsData.json", drop_list=None)


if __name__ == "__main__":
    run()
