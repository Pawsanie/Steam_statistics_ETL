from os import walk, path
from datetime import date
from pathlib import PurePath

from pandas import DataFrame
from luigi import Parameter, DateParameter, LocalTarget

from .Universal_luigi_task import UniversalLuigiTask
from .GetSteamProductsDataInfo_luigi_task import GetSteamProductsDataInfoTask
from .Logging_Config import logging_config
"""
Contains code for luigi tasks: 'SteamAppInfoCSVJoiner', 'SteamDLCInfoCSVJoiner'.
"""


class SteamProductsInfoInfoCSVJoinerTask(UniversalLuigiTask):
    """
    Merges all raw CSV tables into one MasterData for Steam Apps or DLC.
    """
    # Luigi parameters:
    landing_path_part: str = Parameter(
        significant=True,
        description='Root path for landing task result.')
    file_mask: str = Parameter(
        significant=True,
        description='File format for landing.')
    ancestor_file_mask: str = Parameter(
        significant=True,
        description='File format for extract.')
    date_path_part: date = DateParameter(
        significant=True,
        default=date.today(),
        description='Date for root path')
    file_name: str = Parameter(
        significant=True,
        description='File name for landing.')
    # Luigi loging parameters:
    logfile_path: str = Parameter(
        default="steam_products_info.log",
        description='Path to ".log" file')
    loglevel: int = Parameter(
        default=30,
        description='Log Level')
    # Task settings:
    task_namespace: str = 'SteamProductsInfo'
    priority: int = 100
    # Landing path settings:
    directory_for_csv_join: str = 'ProductsInfo'
    # Merge settings:
    # merge_columns_names: list[str] = ['app_id']

    # 'apps_which_are_not_game_list' needs to be supplemented, according to test results ->
    apps_which_are_not_game: list[str] = [
        'Animation & Modeling', 'Game Development', 'Tutorial'
    ]

    def requires(self):
        return {'GetSteamProductsDataInfo': GetSteamProductsDataInfoTask()}

    def output(self) -> LocalTarget:
        """
        Specific Luigi.output method for this Task.
        """
        output_path: str = path.join(*[
            str(self.landing_path_part),
            self.directory_for_csv_join,
            self.get_date_path_part()
        ])
        return LocalTarget(path.join(*[output_path, self.success_flag]))

    def get_csv_for_join(self):
        """
        Creates a root path for csv.
        Then it parses it to get all csv tables to merge.

        Result: dict[str, DataFrame]
        """
        self.interested_partition: dict[str, dict[str]] = {}
        self.interested_data: dict[str, DataFrame] = {}

        result_path: str = self.result_successor.path
        cut_off_path: tuple[str] = PurePath(result_path).parts
        cut_off_path: str = path.join(*[
            cut_off_path[-4],
            cut_off_path[-3],
            cut_off_path[-2],
            cut_off_path[-1]
        ])
        root_path: str = result_path.replace(cut_off_path, '')
        for dirs, folders, files in walk(root_path):
            for folder in folders:
                if self.directory_for_csv_join in folder:
                    for file in files:
                        if self.success_flag in files:
                            if path.isfile(path.join(*[dirs, folder, self.success_flag])):
                                for nested_dirs, nested_folders, nested_files in walk(path.join(*[dirs, folder])):
                                    for nested_file in nested_files:
                                        path_to_file: str = path.join(*[dirs, folder, nested_file])
                                        if path.isfile(path_to_file) and self.ancestor_file_mask in nested_file:

                                            self.interested_partition.setdefault(
                                                self.date_path_part, {}) \
                                                .update(
                                                {
                                                    nested_file: path_to_file
                                                }
                                            )

        self.interested_data: dict[str, DataFrame] = self.task_table_data_parser()

    def steam_apps_data_cleaning(self, all_apps_data_frame: DataFrame) -> DataFrame:
        """
        Clears all_apps_data_frame from apps that are not games.
        """
        apps_which_are_not_game_str: str = '|'.join(self.apps_which_are_not_game)
        all_apps_data_frame: DataFrame = all_apps_data_frame[
            ~all_apps_data_frame['tags'].str.contains(
                apps_which_are_not_game_str,
                regex=True)]\
            .reset_index(drop=True)
        return all_apps_data_frame

    def run(self):
        # Logging settings:
        logging_config(self.logfile_path, int(self.loglevel))
        # Result Successor:
        self.result_successor = self.input()['GetSteamProductsDataInfo']
        # Path settings:
        self.date_path_part: str = self.get_date_path_part()
        # Run:
        self.get_csv_for_join()
        all_apps_data_frame: None = None
        for data in self.interested_data.values():
            all_apps_data_frame: DataFrame = self.data_frames_merge(
                data_from_files=all_apps_data_frame,
                extracted_data=data)
        all_apps_data_frame: DataFrame = self.steam_apps_data_cleaning(all_apps_data_frame)
        # Path settings:
        self.output_path: str = path.join(*[
            self.landing_path_part,
            self.directory_for_csv_join,
            self.date_path_part
        ])
        # self.task_data_landing(
        #     data_to_landing=all_apps_data_frame.replace(r'^\s*$', 'NULL', regex=True),
        #     output_path=self.output_path,
        #     file_name=self.file_name
        # )
