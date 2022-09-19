from os import walk

from pandas import DataFrame

from .Universal_steam_statistics_luigi_task import my_beautiful_task_universal_parser_part
"""
Contains code for luigi task 'AppInfoCSVJoiner'.
"""


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
    # 'apps_which_are_not_game_list' требует дополнения, по результатам тестирования ->
    apps_which_are_not_game = ['Animation & Modeling', 'Game Development', 'Tutorial']
    apps_which_are_not_game_str = '|'.join(apps_which_are_not_game)
    all_apps_data_frame = all_apps_data_frame[~all_apps_data_frame['tags'].str.contains(
        apps_which_are_not_game_str, regex=True)].reset_index(drop=True)
    return all_apps_data_frame
