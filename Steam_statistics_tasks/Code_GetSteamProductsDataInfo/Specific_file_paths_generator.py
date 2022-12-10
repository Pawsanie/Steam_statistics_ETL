from os import path
"""
Contains the SpecificFilePathsGenerator code.
"""


class SpecificFilePathsGenerator:
    """
    Generate paths for GetSteamProductsDataInfoTask.parsing_steam_data methods cash files.
    """
    output_path: str = ''  # Get from GetSteamProductsDataInfoTask.

    def apps_safe_dict_data_path(self):
        return path.join(*[self.output_path, 'Apps_info', '_safe_dict_apps_data'])

    def dlc_safe_dict_data_path(self):
        return path.join(*[self.output_path, 'DLC_info', '_safe_dict_dlc_data'])

    def unsuitable_region_products_df_safe_dict_data_path(self):
        return path.join(*[self.output_path,
                           'Products_not_for_this_region_info',
                           '_safe_dict_products_not_for_this_region_data'])

    def products_not_for_unlogged_user_df_safe_dict_data_path(self):
        return path.join(*[self.output_path,
                           'Products_not_for_unlogged_user_info',
                           '_safe_dict_must_be_logged_to_scrapping_products'])
