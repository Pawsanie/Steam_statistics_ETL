from datetime import date
from os import path, makedirs, remove
from random import randint, uniform

from pandas import DataFrame
from luigi import Parameter, DateParameter, LocalTarget

from .Logging_Config import logging_config
from .Universal_steam_statistics_luigi_task import UniversalLuigiTask
from .AllSteamProductsData_steam_statistics_luigi_task import AllSteamProductsDataTask
from .Code_GetSteamProductsDataInfo.Parsing_steam_data import ParsingSteamData
from .Code_GetSteamProductsDataInfo.Specific_file_paths_generator import SpecificFilePathsGenerator
"""
Contains code for luigi task 'GetSteamProductsInfo'.
"""


class GetSteamProductsDataInfoTask(UniversalLuigiTask, ParsingSteamData, SpecificFilePathsGenerator):
    """
    Parses and scrapes the list of products available on Steam.
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
    # Luigi loging parameters:
    logfile_path: str = Parameter(
        default="steam_products_data_info.log",
        description='Path to ".log" file')
    loglevel: int = Parameter(
        default=30,
        description='Log Level')
    # Task settings:
    task_namespace: str = 'GetSteamProductsDataInfo'
    priority: int = 5000
    retry_count: int = 1
    # Collections base values:
    is_not_application_list: list[str] = [
        'Soundtrack', 'OST', 'Artbook', 'Texture', 'Demo', 'Playtest',
        'test2', 'test3', 'Pieterw', 'Closed Beta', 'Open Beta', 'RPG Maker',
        'Pack', 'Trailer', 'Teaser', 'Digital Art Book', 'Preorder Bonus'
    ]
    # Wait settings:
    # time_wait: float = uniform(0.1, 0.3)
    time_wait: int = randint(1, 3)
    #
    # def requires(self):
    #     """
    #     Standard Luigi.Task.requires method.
    #     """
    #     return {'AllSteamProductsData': AllSteamProductsDataTask()}
    #
    # def output(self) -> LocalTarget:
    #     """
    #     Standard Luigi.Task.output method.
    #     """
    #     return LocalTarget(path.join(*[self.output_path, self.success_flag]))

    def steam_apps_parser(self) -> DataFrame:
        """
        Delete what is not a game at the stage of working with raw data.
        """
        is_not_application_str: str = '|'.join(self.is_not_application_list)
        result_df: None = None
        for value in self.interested_data:
            interested_data = self.interested_data.get(value)
            interested_data = interested_data[
                ~interested_data['name']
                .str.contains(is_not_application_str, regex=True)
            ]
            null_filter = interested_data['name'] != ""
            result_df: DataFrame = self.data_frames_merge(
                data_from_files=result_df,
                extracted_data=interested_data[null_filter])
        return result_df.reset_index(drop=True)

    def make_flag(self, partition_path: str):
        """
        Maike flags for empty collections.
        """
        output_path: str = f'{partition_path}'
        if not path.exists(output_path):
            makedirs(output_path)
        flag_path: str = path.join(*[output_path, self.success_flag])
        with open(flag_path, 'w'):
            pass

    def delete_temporary_safe_files(self, products_dict: dict[str]):
        """
        Delete temporary files for task.
        """
        for key in products_dict:
            file_path: str = self.product_save_file_path(key, products_dict.get(key))
            if path.isfile(file_path):
                remove(file_path)

    def apps_and_dlc_df_landing(self, apps_df: DataFrame, apps_df_save_path: str,
                                dlc_df: DataFrame, dlc_df_save_path: str,
                                unsuitable_region_products_df: DataFrame, unsuitable_region_products_df_path: str,
                                products_not_for_unlogged_user_df: DataFrame,
                                products_not_for_unlogged_user_df_path: str):
        """
        Lands real collections and maike flags if theme empty.
        """
        data_for_landing: dict = {
            f"{'Steam_Apps_Info'}": [
                apps_df, apps_df_save_path
            ],
            f"{'Steam_DLC_Info'}": [
                dlc_df, dlc_df_save_path
            ],
            f"{'Unsuitable_region_Products_Info'}": [
                unsuitable_region_products_df, unsuitable_region_products_df_path
            ],
            f"{'Products_not_for_unlogged_user_Info'}": [
                products_not_for_unlogged_user_df, products_not_for_unlogged_user_df_path]
        }
        for key in data_for_landing:
            if len(data_for_landing.get(key)[0]) != 0:
                self.task_data_landing(
                    data_to_landing=data_for_landing.get(key)[0].replace(r'^\s*$', 'NULL', regex=True),
                    output_path=data_for_landing.get(key)[1],
                    file_name=key)
            else:
                self.make_flag(data_for_landing.get(key)[1])

    def run(self):
        # Path settings:
        self.date_path_part: str = self.get_date_path_part()
        self.output_path: str = path.join(*[str(self.landing_path_part), self.date_path_part])
        # Result Successor:
        self.result_successor: str = self.input()['AllSteamProductsData']
        # Logging settings:
        logging_config(self.logfile_path, int(self.loglevel))

        # Run:
        self.interested_data: dict[str, DataFrame] = self.get_extract_data(requires=self.result_successor)
        self.interested_data: DataFrame = self.steam_apps_parser()

        # list[DataFrame]
        apps_df, dlc_df, unsuitable_region_products_df, products_not_for_unlogged_user_df = self.parsing_steam_data()

        # list[str]
        apps_df_save_path, dlc_df_save_path, unsuitable_region_products_df_path, products_not_for_unlogged_user_df_path\
            = self.get_product_save_file_path_list([
                'Apps_info',
                'DLC_info',
                'Products_not_for_this_region_info',
                'Products_not_for_unlogged_user_info'
            ])

        self.apps_and_dlc_df_landing(
            apps_df,
            apps_df_save_path,
            dlc_df,
            dlc_df_save_path,
            unsuitable_region_products_df,
            unsuitable_region_products_df_path,
            products_not_for_unlogged_user_df,
            products_not_for_unlogged_user_df_path
        )
        self.delete_temporary_safe_files({
            'Apps_info': '_safe_dict_apps_data',
            'DLC_info': '_safe_dict_dlc_data',
            'Products_not_for_this_region_info': '_safe_dict_products_not_for_this_region_data',
            "Products_not_for_unlogged_user_info": "_safe_dict_must_be_logged_to_scrapping_products"
        })

        self.make_flag(self.output_path)
