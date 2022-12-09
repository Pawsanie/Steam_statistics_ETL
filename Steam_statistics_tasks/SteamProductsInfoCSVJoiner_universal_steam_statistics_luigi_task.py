from os import walk

from pandas import DataFrame

from .Universal_steam_statistics_luigi_task import my_beautiful_task_universal_parser_part, \
    my_beautiful_task_data_frame_merge, my_beautiful_task_data_landing
"""
Contains code for luigi tasks: 'SteamAppInfoCSVJoiner', 'SteamDLCInfoCSVJoiner'.
"""


def steam_products_info_run(self, steam_info_path):
    """
    Function for Luigi.Task.run()
    Need "directory_for_csv_join and csv_file_for_result" variables in class.
    """
    result_successor = self.input()['GetSteamProductsDataInfo']
    interested_data: dict[DataFrame] = get_csv_for_join(result_successor, self.directory_for_csv_join)
    all_apps_data_frame = None
    for data in interested_data.values():
        all_apps_data_frame: DataFrame = my_beautiful_task_data_frame_merge(all_apps_data_frame, data)
    all_apps_data_frame: DataFrame = steam_apps_data_cleaning(all_apps_data_frame)
    day_for_landing = f"{self.date_path_part:%Y/%m/%d}"
    my_beautiful_task_data_landing(all_apps_data_frame, f"{steam_info_path}/{day_for_landing}",
                                   self.csv_file_for_result)


def get_csv_for_join(result_successor, catalog: str) -> dict[DataFrame]:
    """
    Creates a root path for csv.
    Then it parses it to get all csv tables to merge.
    """
    result_path = result_successor.path
    cut_off_path = result_path.split('/')
    cut_off_path = f"{cut_off_path[-4]}/{cut_off_path[-3]}/{cut_off_path[-2]}/{cut_off_path[-1]}"
    root_path, file_list = result_path.replace(cut_off_path, ''), []
    for dirs, folders, files in walk(root_path):
        if catalog in dirs:
            for file in files:
                path_to_file = f'{dirs}/{file}'
                file_list.append(path_to_file)
    interested_data: dict[DataFrame] = my_beautiful_task_universal_parser_part(file_list, '.csv')
    return interested_data


def steam_apps_data_cleaning(all_apps_data_frame: DataFrame) -> DataFrame:
    """
    Clears all_apps_data_frame from apps that are not games.
    """
    # 'apps_which_are_not_game_list' needs to be supplemented, according to test results ->
    apps_which_are_not_game = ['Animation & Modeling', 'Game Development', 'Tutorial']
    apps_which_are_not_game_str = '|'.join(apps_which_are_not_game)
    all_apps_data_frame = all_apps_data_frame[~all_apps_data_frame['tags'].str.contains(
        apps_which_are_not_game_str, regex=True)].reset_index(drop=True)
    return all_apps_data_frame


class AllSteamProductsData():
    ...
