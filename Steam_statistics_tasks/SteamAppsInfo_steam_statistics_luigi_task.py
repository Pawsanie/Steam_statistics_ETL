from pandas import DataFrame
from os import walk
from .Universal_steam_statistics_luigi_task import my_beautiful_task_universal_parser_part
"""
Contains code for luigi task 'AppInfoCSVJoiner'.
"""


def get_csv_for_join(result_successor) -> dict:
    """
    Creates a root path for csv.
    Then it parses it to get all csv tables to merge.
    '''
    Создаёт корневой путь для csv.
    Затем парсит его, с целью получить все csv таблицы для объединения.
    """
    root_path = result_successor.path
    symbol_counts = len(root_path)
    root_path = root_path[:symbol_counts - 28]
    file_list = []
    for dirs, folders, files in walk(root_path):
        for file in files:
            path_to_file = f'{dirs}/{file}'
            file_list.append(path_to_file)
    interested_data = my_beautiful_task_universal_parser_part(file_list, '.csv', drop_list=None)
    return interested_data


def steam_apps_data_cleaning(all_apps_data_frame) -> DataFrame:
    """
    Clears all_apps_data_frame from apps that are not games.
    '''
    Очищает all_apps_data_frame от приложений, которые не являются играми.
    """
    # 'apps_which_are_not_game' требует дополнения, по результатам тестирования ->
    apps_which_are_not_game = ['Animation & Modeling', 'Game Development', 'Tutorial']
    all_apps_data_frame_heads = all_apps_data_frame.head()
    for index in range(len(all_apps_data_frame)):
        for column_name in all_apps_data_frame_heads:
            column_name = str(column_name)
            column_name = all_apps_data_frame.iloc[index][column_name]
            if str(column_name) in apps_which_are_not_game:
                all_apps_data_frame = all_apps_data_frame.drop(all_apps_data_frame.index[index], inplace=True)
    all_apps_data_frame = all_apps_data_frame.reset_index(drop=True)
    return all_apps_data_frame
