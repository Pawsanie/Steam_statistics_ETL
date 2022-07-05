from os import walk
from pandas import DataFrame, concat
from .Universal_steam_statistics_luigi_task import my_beautiful_task_universal_parser_part, \
    my_beautiful_task_data_frame_merge
"""
Contains code for luigi task 'AllSteamAppsData'.
"""


def steam_aps_from_web_api_parser(interested_data) -> dict:
    """
    Parses the result received from the Steam Web-API.
    '''
    Парсит результат получаемый от Steam Web-API.
    """
    all_aps_data = interested_data
    all_aps_data = all_aps_data.get('applist')
    all_aps_data = all_aps_data.get('apps')
    return all_aps_data


def steam_apps_validator(steam_apps_list, partition_path) -> DataFrame:
    """
    If the result of work 'AllSteamAppsData' already exists, checks it for duplicates
    and saves in the last iteration only new product available on Steam.
    '''
    Если результат работы 'AllSteamAppsData' уже существует, то проверяет его на дубли
    и сохраняет в последнюю итерацию лишь новые товары доступные в Steam.
    """
    file_list = []
    for dirs, folders, files in walk(partition_path):
        for file in files:
            path_to_file = f'{dirs}/{file}'
            file_list.append(path_to_file)
    if len(file_list) != 0:
        interested_data = my_beautiful_task_universal_parser_part(file_list, '.json', drop_list=None)
        all_apps_parsing_data = None
        for data in interested_data.values():
            all_apps_parsing_data = my_beautiful_task_data_frame_merge(all_apps_parsing_data, data)
        new_steam_apps_list = DataFrame(steam_apps_list)
        interested_apps = concat([all_apps_parsing_data, new_steam_apps_list]).drop_duplicates(keep=False)
        interested_apps = interested_apps.reset_index(drop=True)
    else:
        interested_apps = steam_apps_list
    return interested_apps
