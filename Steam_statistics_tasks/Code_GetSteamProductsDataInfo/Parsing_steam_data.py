from random import randint
from time import sleep
import logging

from pandas import DataFrame, concat
from tqdm import tqdm


from .Specific_file_paths_generator import SpecificFilePathsGenerator
from .Local_cash_reader import LocalCashReader
from .Scraping_steam_data import ScrapingSteamData
"""
Contains the ParsingSteamData code.
"""


class ParsingSteamData(SpecificFilePathsGenerator, LocalCashReader, ScrapingSteamData):
    # Collections base values:
    apps_df: DataFrame or None = None
    dlc_df: DataFrame or None = None
    unsuitable_region_products_df: DataFrame or None = None
    products_not_for_unlogged_user_df: DataFrame or None = None
    interested_data: DataFrame = DataFrame()
    # Wait settings:
    time_wait: int = randint(1, 3)

    def __init__(self):
        self.apps_df_redy: DataFrame = DataFrame()
        self.dlc_df_redy: DataFrame = DataFrame()
        self.unsuitable_region_products_df_redy: DataFrame = DataFrame()
        self.products_not_for_unlogged_user_df_redy: DataFrame = DataFrame()

    def local_cash_read(self):
        self.apps_df_redy: DataFrame = self.data_from_file_to_pd_dataframe(self.apps_safe_dict_data_path())
        self.dlc_df_redy: DataFrame = self.data_from_file_to_pd_dataframe(self.dlc_safe_dict_data_path())
        self.unsuitable_region_products_df_redy: DataFrame = \
            self.data_from_file_to_pd_dataframe(self.unsuitable_region_products_df_safe_dict_data_path())
        self.products_not_for_unlogged_user_df_redy: DataFrame = \
            self.data_from_file_to_pd_dataframe(self.products_not_for_unlogged_user_df_safe_dict_data_path())



    def parsing_steam_data(self, get_steam_app_info_path: str, day_for_landing: str) -> list[DataFrame]:
        """
        Root function responsible for reading the local cache and
        merge it with parsed data from scraping steam application pages.
        Responsible for timeouts of get requests to application pages.
        """
        self.local_cash_read()

        all_products_data_redy = concat([
            self.apps_df_redy['app_name'],
            self.dlc_df_redy['app_name'],
            self.unsuitable_region_products_df_redy['app_name'],
            self.products_not_for_unlogged_user_df_redy['app_name']]
        ).drop_duplicates().values

        common_all_products_data_redy = self.interested_data.merge(
            DataFrame({'name': all_products_data_redy}),
            on=['name'])

        interested_products = self.interested_data[
            ~self.interested_data.name.isin(
                common_all_products_data_redy.name)]\
            .drop_duplicates().reset_index(drop=True)

        for index, tqdm_percent in zip(range(len(interested_products)),
                                       tqdm(range(len(interested_products) + len(common_all_products_data_redy)),
                                            desc="Scraping Steam products",
                                            unit=' SteamApp',
                                            ncols=120,
                                            # colour='green',
                                            initial=len(common_all_products_data_redy))):

            app_name = interested_products.iloc[index]['name']
            app_id = interested_products.iloc[index]['appid']

            if str(app_name) not in common_all_products_data_redy['name'].values:

                sleep(self.time_wait)
                result_list: list[dict, dict, bool] = self.ask_app_in_steam_store(app_id, app_name)
                result, result_dlc, must_be_logged = result_list[0], result_list[1], result_list[2]

                if must_be_logged is False:
                    if int(len(result) + len(result_dlc)) > 0:
                        # App scraping result validate.
                        apps_df: DataFrame = steam_product_scraping_validator(result, get_steam_app_info_path,
                                                                              day_for_landing, self.apps_df, app_name,
                                                                              '_safe_dict_apps_data', 'Apps_info', )
                        # DLC scraping result validate.
                        dlc_df: DataFrame = steam_product_scraping_validator(result_dlc, get_steam_app_info_path,
                                                                             day_for_landing, self.dlc_df, app_name,
                                                                             '_safe_dict_dlc_data', 'DLC_info')
                    else:  # Product not available in this region!  # BAG!
                        unsuitable_region_products_df: DataFrame = \
                            unsuitable_products(app_id, app_name, get_steam_app_info_path,
                                                day_for_landing, self.unsuitable_region_products_df,
                                                '_safe_dict_products_not_for_this_region_data',
                                                'Products_not_for_this_region_info',
                                                'product is not available in this region...')
                else:  # Fake user must be logged in steam for scraping this product page.
                    products_not_for_unlogged_user_df: DataFrame = \
                        unsuitable_products(app_id, app_name, get_steam_app_info_path,
                                            day_for_landing, self.products_not_for_unlogged_user_df,
                                            '_safe_dict_must_be_logged_to_scrapping_products',
                                            'Products_not_for_unlogged_user_info',
                                            'product is not available for unlogged user...')
            else:
                logging.info("'" + app_name + "' already is in _safe_*_data...")
        apps_and_dlc_df_list: list[DataFrame] = apps_and_dlc_list_validator(
            self.apps_df,
            self.apps_df_redy,
            self.dlc_df,
            self.dlc_df_redy,
            self.unsuitable_region_products_df,
            self.unsuitable_region_products_df_redy,
            self.products_not_for_unlogged_user_df,
            self.products_not_for_unlogged_user_df_redy
        )
        return apps_and_dlc_df_list
