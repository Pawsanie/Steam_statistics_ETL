from datetime import datetime, date
from os import path, makedirs, remove
import logging
from random import randint, uniform

from pandas import DataFrame
from luigi import Parameter, DateParameter



from .Logging_Config import logging_config
from .Universal_steam_statistics_luigi_task import UniversalLuigiTask
from .AllSteamProductsData_steam_statistics_luigi_task import AllSteamProductsDataTask

"""
Contains code for luigi task 'GetSteamProductsInfo'.
"""


def safe_dict_data(path_to_file: str, date: str, df: DataFrame, file_name: str, ds_name: str):
    """
    Temporary storage, for landing raw data.
    """
    path_to_file = f"{path_to_file}/{date}/{ds_name}"
    file_path = f"{path_to_file}/{file_name}"
    df = df.to_dict('records')
    df = str(df[0]) + '\n'
    if not path.exists(path_to_file):
        makedirs(path_to_file)
    with open(file_path, 'a') as safe_file:
        safe_file.write(df)





def make_flag(partition_path: str):
    """
    Maike flags for empty collections.
    """
    output_path = f'{partition_path}'
    if not path.exists(output_path):
        makedirs(output_path)
    flag_path = f'{output_path}/{"_Validate_Success"}'
    with open(flag_path, 'w'):
        pass


def apps_and_dlc_df_landing(apps_df: DataFrame, apps_df_save_path: str,
                            dlc_df: DataFrame, dlc_df_save_path: str,
                            unsuitable_region_products_df: DataFrame, unsuitable_region_products_df_path: str,
                            products_not_for_unlogged_user_df: DataFrame, products_not_for_unlogged_user_df_path: str):
    """
    Lands real collections and maike flags if theme empty.
    """
    data_for_landing = {"Steam_Apps_Info.csv": [apps_df, apps_df_save_path],
                        "Steam_DLC_Info.csv": [dlc_df, dlc_df_save_path],
                        "Unsuitable_region_Products_Info.csv": [unsuitable_region_products_df,
                                                                unsuitable_region_products_df_path],
                        "Products_not_for_unlogged_user_Info.csv": [products_not_for_unlogged_user_df,
                                                                    products_not_for_unlogged_user_df_path]}
    for key in data_for_landing:
        if len(data_for_landing.get(key)[0]) != 0:
            my_beautiful_task_data_landing(data_for_landing.get(key)[0].replace(r'^\s*$', 'NULL', regex=True),
                                           data_for_landing.get(key)[1], key)
        else:
            make_flag(data_for_landing.get(key)[1])


def apps_and_dlc_list_validator(apps_df: DataFrame, apps_df_redy: DataFrame,
                                dlc_df: DataFrame, dlc_df_redy: DataFrame,
                                unsuitable_region_products_df: DataFrame,
                                unsuitable_region_products_df_redy: DataFrame,
                                products_not_for_unlogged_user_df: DataFrame,
                                products_not_for_unlogged_user_df_redy: DataFrame
                                ) -> list[DataFrame]:
    """
    Pandas DataFrame validator.
    Checks that the application and DLC collections are not empty.
    Merge real collections to land on.
    """
    data_for_validation = {"Steam_Apps_Info": [apps_df, apps_df_redy],
                           "Steam_DLC_Info": [dlc_df, dlc_df_redy],
                           "Unsuitable_region_Products_Info": [unsuitable_region_products_df,
                                                               unsuitable_region_products_df_redy],
                           "Products_not_for_unlogged_user_Info": [products_not_for_unlogged_user_df,
                                                                   products_not_for_unlogged_user_df_redy]}
    apps_and_dlc_df_list = []
    for key in data_for_validation:
        if type(data_for_validation.get(key)[0]) == type(None):
            apps_and_dlc_df_list.append([])
        else:
            apps_and_dlc_df_list.append(my_beautiful_task_data_frame_merge(data_for_validation.get(key)[0],
                                                                           data_for_validation.get(key)[1]))
    return apps_and_dlc_df_list


def result_column_sort(interest_dict: dict) -> DataFrame:
    """
    Sort columns and mayke DF from apps or DLC dict.
    """
    new_df_row = DataFrame.from_dict(interest_dict)
    inserted_columns = ['app_id', 'app_name', 'developer', 'rating_all_time_percent',
                        'rating_all_time_count', 'rating_30d_percent', 'rating_30d_count',
                        'publisher', 'price', 'steam_release_date', 'tags', 'scan_date']
    row_data_frame_head = new_df_row.head()
    for column_name in inserted_columns:
        if column_name not in row_data_frame_head:
            new_df_row[column_name] = ''
    new_df_row = new_df_row[inserted_columns]
    return new_df_row


def steam_product_scraping_validator(scraping_result: dict[str], get_steam_product_info_path: str, day_for_landing: str,
                                     product_data_frame: DataFrame, app_name: str, safe_name: str,
                                     catalogue_name: str
                                     ) -> DataFrame:
    """
    Validate scraping status of steam product then safe a result.
    """
    if scraping_result is not None and len(scraping_result) != 0:
        new_df_row: DataFrame = result_column_sort(scraping_result)
        safe_dict_data(get_steam_product_info_path, day_for_landing, new_df_row, safe_name, catalogue_name)
        product_data_frame: DataFrame = my_beautiful_task_data_frame_merge(product_data_frame, new_df_row)
        logging.info("'" + app_name + "' scraping succsessfully completed.")
    return product_data_frame


def unsuitable_products(app_id: str, app_name: str, get_steam_app_info_path: str,
                        day_for_landing: str, unsuitable_products_df: DataFrame,
                        safe_file_name: str, unsuitable_product_catalog: str,
                        logg_massage: str) -> DataFrame:
    """
    Unsuitable product safe, add to DataFrame and logging info massage.
    """
    date_today = str(datetime.today()).split(' ')
    new_df_row = DataFrame(data={'app_id': [app_id], 'app_name': [app_name], 'scan_date': [date_today[0]]})
    safe_dict_data(get_steam_app_info_path, day_for_landing, new_df_row,
                   safe_file_name, unsuitable_product_catalog)
    unsuitable_products_df: DataFrame = \
        my_beautiful_task_data_frame_merge(unsuitable_products_df, new_df_row)
    logging.info("'" + app_name + "' " + logg_massage)
    return unsuitable_products_df


def parsing_steam_data(interested_data: DataFrame, get_steam_app_info_path: str, day_for_landing: str,
                       apps_df: DataFrame or None, dlc_df: DataFrame or None,
                       unsuitable_region_products_df: DataFrame or None,
                       products_not_for_unlogged_user_df: DataFrame or None) -> list[DataFrame]:
    """
    Root function responsible for reading the local cache and
    merge it with parsed data from scraping steam application pages.
    Responsible for timeouts of get requests to application pages.
    """


            result_list: list[dict, dict, bool] = ask_app_in_steam_store(app_id, app_name)
            result, result_dlc, must_be_logged = result_list[0], result_list[1], result_list[2]

            if must_be_logged is False:
                if int(len(result) + len(result_dlc)) > 0:
                    # App scraping result validate.
                    apps_df: DataFrame = steam_product_scraping_validator(result, get_steam_app_info_path,
                                                                          day_for_landing, apps_df, app_name,
                                                                          '_safe_dict_apps_data', 'Apps_info',)
                    # DLC scraping result validate.
                    dlc_df: DataFrame = steam_product_scraping_validator(result_dlc, get_steam_app_info_path,
                                                                         day_for_landing, dlc_df, app_name,
                                                                         '_safe_dict_dlc_data', 'DLC_info')
                else:  # Product not available in this region!  # BAG!
                    unsuitable_region_products_df: DataFrame = \
                        unsuitable_products(app_id, app_name, get_steam_app_info_path,
                                            day_for_landing, unsuitable_region_products_df,
                                            '_safe_dict_products_not_for_this_region_data',
                                            'Products_not_for_this_region_info',
                                            'product is not available in this region...')
            else:  # Fake user must be logged in steam for scraping this product page.
                products_not_for_unlogged_user_df: DataFrame = \
                    unsuitable_products(app_id, app_name, get_steam_app_info_path,
                                        day_for_landing, products_not_for_unlogged_user_df,
                                        '_safe_dict_must_be_logged_to_scrapping_products',
                                        'Products_not_for_unlogged_user_info',
                                        'product is not available for unlogged user...')
        else:
            logging.info("'" + app_name + "' already is in _safe_*_data...")
    apps_and_dlc_df_list: list[DataFrame] = apps_and_dlc_list_validator(apps_df, apps_df_redy, dlc_df, dlc_df_redy,
                                                                        unsuitable_region_products_df,
                                                                        unsuitable_region_products_df_redy,
                                                                        products_not_for_unlogged_user_df,
                                                                        products_not_for_unlogged_user_df_redy)
    return apps_and_dlc_df_list


def product_save_file_path(self, product: str, save_file: str) -> str:
    """
    Path generator.
    """
    return f"{self.get_steam_products_data_info_path}/{self.date_path_part:%Y/%m/%d}/{product}/{save_file}"


def get_product_save_file_path_list(self, products_save_file_list: list[str]) -> list[str]:
    """
    Path multy-generator.
    """
    result = []
    for dir_name in products_save_file_list:
        result.append(product_save_file_path(self, dir_name, ''))
    return result


def delete_temporary_safe_files(self, products_dict: dict[str]):
    """
    Delete temporary files for task.
    """
    for key in products_dict:
        file_path: str = product_save_file_path(self, key, products_dict.get(key))
        if path.isfile(file_path):
            remove(file_path)


def get_steam_products_data_info_steam_statistics_luigi_task_run(self):
    """
    Function for Luigi.Task.run()
    """


    apps_and_dlc_df_list: list[DataFrame] = parsing_steam_data(interested_data,
                                                               self.get_steam_products_data_info_path,
                                                               day_for_landing, apps_df, dlc_df,
                                                               unsuitable_region_products_df,
                                                               products_not_for_unlogged_user_df)
    apps_df, dlc_df, unsuitable_region_products_df, products_not_for_unlogged_user_df = \
        apps_and_dlc_df_list[0], apps_and_dlc_df_list[1], apps_and_dlc_df_list[2], apps_and_dlc_df_list[3]

    product_save_file_path_list: list[str] = \
        get_product_save_file_path_list(self, ['Apps_info', 'DLC_info', 'Products_not_for_this_region_info',
                                               'Products_not_for_unlogged_user_info'])
    apps_df_save_path, dlc_df_save_path, unsuitable_region_products_df_path, products_not_for_unlogged_user_df_path = \
        product_save_file_path_list[0], product_save_file_path_list[1], \
        product_save_file_path_list[2], product_save_file_path_list[3]

    apps_and_dlc_df_landing(apps_df, apps_df_save_path, dlc_df, dlc_df_save_path,
                            unsuitable_region_products_df, unsuitable_region_products_df_path,
                            products_not_for_unlogged_user_df, products_not_for_unlogged_user_df_path)

    delete_temporary_safe_files(self, {'Apps_info': '_safe_dict_apps_data',
                                       'DLC_info': '_safe_dict_dlc_data',
                                       'Products_not_for_this_region_info':
                                           '_safe_dict_products_not_for_this_region_data',
                                       "Products_not_for_unlogged_user_info":
                                           "_safe_dict_must_be_logged_to_scrapping_products"})

    make_flag(f"{self.get_steam_products_data_info_path}/{day_for_landing}")


class GetSteamProductsDataInfoTask(UniversalLuigiTask):
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
    file_name: str = Parameter(
        significant=True,
        description='File name for landing.')
    date_path_part: date = DateParameter(
        default=date.today(),
        description='Date for root path')
    # Luigi loging parameters:
    get_steam_products_data_info_logfile_path: str = Parameter(
        default="steam_products_data_info.log",
        description='Path to ".log" file')
    get_steam_products_data_info_loglevel: int = Parameter(
        default=30,
        description='Log Level')
    # Task settings:
    task_namespace: str = 'GetSteamProductsDataInfo'
    priority: int = 5000
    # Collections base values:
    is_not_application_list = [
        'Soundtrack', 'OST', 'Artbook', 'Texture', 'Demo', 'Playtest',
        'test2', 'test3', 'Pieterw', 'Closed Beta', 'Open Beta', 'RPG Maker',
        'Pack', 'Trailer', 'Teaser', 'Digital Art Book', 'Preorder Bonus'
    ]
    # Wait settings:
    # time_wait = randint(1, 3)
    time_wait = uniform(0.1, 0.3)

    def requires(self):
        """
        Standard Luigi.Task.requires method.
        """
        return {'AllSteamProductsData': AllSteamProductsDataTask()}

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

    def run(self):
        # Path settings:
        self.date_path_part: str = self.get_date_path_part()
        self.output_path: str = path.join(*[str(self.landing_path_part), self.date_path_part])
        # Result Successor:
        self.result_successor = self.input()['AllSteamProductsData']
        # Logging settings:
        logging_config(self.get_steam_products_data_info_logfile_path, int(self.get_steam_products_data_info_loglevel))
        # Run:
        self.interested_data: dict[str, DataFrame] = self.get_extract_data(requires=self.result_successor)
        self.interested_data: DataFrame = self.steam_apps_parser()
