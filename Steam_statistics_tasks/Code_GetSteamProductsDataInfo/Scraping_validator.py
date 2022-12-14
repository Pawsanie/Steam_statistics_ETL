import logging
from os import path, makedirs
from datetime import datetime

from pandas import DataFrame

from ..Universal_luigi_task import DataFramesMerge
"""
Contains the ScrapingValidator code.
"""


class ScrapingValidator(DataFramesMerge):
    """
    Validate scraping status of steam product then safe a result.
    """
    # Column names order:
    inserted_columns = [
        'app_id', 'app_name', 'developer', 'rating_all_time_percent',
        'rating_all_time_count', 'rating_30d_percent', 'rating_30d_count',
        'publisher', 'price', 'steam_release_date', 'tags', 'scan_date'
    ]
    # Luigi parameters:
    output_path: str = ''

    def safe_dict_data(self, *, data_frame: DataFrame, file_name: str, catalogue_name: str):
        """
        Temporary storage, for landing raw data.
        """
        path_to_file: str = path.join(*[self.output_path, catalogue_name])
        file_path: str = path.join(*[path_to_file, file_name])
        df: DataFrame = data_frame.to_dict('records')
        df: str = str(df[0]) + '\n'
        if not path.exists(path_to_file):
            makedirs(path_to_file)
        with open(file_path, 'a') as safe_file:
            safe_file.write(df)

    def result_column_sort(self, interest_dict: dict) -> DataFrame:
        """
        Sort columns and mayke DF from apps or DLC dict.
        """
        new_df_row: DataFrame = DataFrame.from_dict(interest_dict)

        row_data_frame_head = new_df_row.head()
        for column_name in self.inserted_columns:
            if column_name not in row_data_frame_head:
                new_df_row[column_name] = ''
        new_df_row: DataFrame = new_df_row[self.inserted_columns]
        return new_df_row

    def steam_product_scraping_validator(self, *, scraping_result: dict[str],
                                         product_data_frame: DataFrame, app_name: str,
                                         safe_name: str, catalogue_name: str) -> DataFrame:
        """
        Validate scraping status of steam product then safe a result.
        """
        if scraping_result is not None and len(scraping_result) != 0:
            new_df_row: DataFrame = self.result_column_sort(scraping_result)
            self.safe_dict_data(
                data_frame=new_df_row,
                file_name=safe_name,
                catalogue_name=catalogue_name
            )
            product_data_frame: DataFrame = self.data_frames_merge(
                data_from_files=product_data_frame,
                extracted_data=new_df_row)
            logging.info("'" + app_name + "' scraping successfully completed.")
        return product_data_frame

    def unsuitable_products(self, *, app_id: str, app_name: str, unsuitable_products_df: DataFrame,
                            safe_file_name: str, unsuitable_product_catalog: str,
                            logg_massage: str) -> DataFrame:
        """
        Unsuitable product safe, add to DataFrame and logging info massage.
        """
        date_today: list[str] = str(datetime.today()).split(' ')
        new_df_row: DataFrame = DataFrame(data={
            'app_id': [app_id],
            'app_name': [app_name],
            'scan_date': [date_today[0]]
        })
        self.safe_dict_data(
            data_frame=new_df_row,
            file_name=safe_file_name,
            catalogue_name=unsuitable_product_catalog
        )
        unsuitable_products_df: DataFrame = self.data_frames_merge(
            data_from_files=unsuitable_products_df,
            extracted_data=new_df_row)
        logging.info("'" + app_name + "' " + logg_massage)
        return unsuitable_products_df

    def apps_and_dlc_list_validator(self, apps_df: DataFrame, apps_df_redy: DataFrame,
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
        data_for_validation: dict = {
            "Steam_Apps_Info": [apps_df, apps_df_redy],
            "Steam_DLC_Info": [dlc_df, dlc_df_redy],
            "Unsuitable_region_Products_Info": [unsuitable_region_products_df,
                                                unsuitable_region_products_df_redy],
            "Products_not_for_unlogged_user_Info": [products_not_for_unlogged_user_df,
                                                    products_not_for_unlogged_user_df_redy]
        }
        apps_and_dlc_df_list: list = []
        for key in data_for_validation:
            if type(data_for_validation.get(key)[0]) == type(None):  # Not fault: must be '=='
                apps_and_dlc_df_list.append([])
            else:
                apps_and_dlc_df_list.append(self.data_frames_merge(
                    data_from_files=data_for_validation.get(key)[0],
                    extracted_data=data_for_validation.get(key)[1])
                )
        return apps_and_dlc_df_list
