from os import walk, path
from datetime import date

from pandas import DataFrame
from luigi import Parameter, DateParameter

from .Universal_steam_statistics_luigi_task import UniversalLuigiTask
from .GetSteamProductsDataInfo_steam_statistics_luigi_task import GetSteamProductsDataInfoTask
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

    directory_for_csv_join: str = ''

    # 'apps_which_are_not_game_list' needs to be supplemented, according to test results ->
    apps_which_are_not_game: list[str] = [
        'Animation & Modeling', 'Game Development', 'Tutorial'
    ]

    def requires(self):
        return {'GetSteamProductsDataInfo': GetSteamProductsDataInfoTask()}

    def get_csv_for_join(self) -> dict[DataFrame]:
        """
        Creates a root path for csv.
        Then it parses it to get all csv tables to merge.
        """
        result_path: str = self.result_successor.path
        cut_off_path: list[str] = result_path.split('/')
        cut_off_path: str = path.join(*[
            cut_off_path[-4],
            cut_off_path[-3],
            cut_off_path[-2],
            cut_off_path[-1]
        ])
        root_path, file_list = result_path.replace(cut_off_path, ''), []
        for dirs, folders, files in walk(root_path):
            if self.directory_for_csv_join in dirs:
                for file in files:
                    path_to_file: str = path.join(*[dirs, file])
                    file_list.append(path_to_file)
        self.result_successor: list[str] = file_list
        interested_data: dict[DataFrame] = self.task_path_parser()
        return interested_data

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
        self.result_successor = self.input()['GetSteamProductsDataInfo']
        interested_data: dict[DataFrame] = self.get_csv_for_join()
        all_apps_data_frame: None = None
        for data in interested_data.values():
            all_apps_data_frame: DataFrame = self.data_frames_merge(
                data_from_files=all_apps_data_frame,
                extracted_data=data)
        all_apps_data_frame: DataFrame = self.steam_apps_data_cleaning(all_apps_data_frame)

        output_path: str = path.join(*[
            self.landing_path_part,
            self.directory_for_csv_join,
            self.get_date_path_part()
        ])
        self.task_data_landing(
            data_to_landing=all_apps_data_frame,
            output_path=output_path,
            file_name=f"{self.file_name}.{self.file_mask}"
        )
