import unittest
from unittest.mock import patch, Mock
from datetime import date

from pandas import DataFrame
from requests import get
import luigi.notifications
from luigi import DateParameter, Parameter

from steam_statistics_luigi_ETL import AllSteamProductsData, GetSteamProductsDataInfo, SteamAppsInfo
from Steam_statistics_tasks.AllSteamProductsData_steam_statistics_luigi_task import *
from Steam_statistics_tasks.GetSteamProductsDataInfo_steam_statistics_luigi_task import *
from Steam_statistics_tasks.SteamAppsInfo_steam_statistics_luigi_task import *
from Steam_statistics_tasks.Universal_steam_statistics_luigi_task import *

"""
Steam statistics Luigi ETL unit tests.
"""

luigi.notifications.DEBUG = True


def steam_server_status():
    """
    Server status test.
    """
    server_satus = get('http://api.steampowered.com/ISteamWebAPIUtil/GetServerInfo/v0001/')
    print('\nSteam WEB-API server satus: ' + str(server_satus))


class TestAllSteamProductsData(unittest.TestCase):
    """
    Test AllSteamProductsData.
    """
    # Parameters without errors:
    test_all_steam_apps_path = '~/Steam_ETL/All_steam_apps'
    test_date_path_part = date(2012, 12, 15)

    # Parameters with errors:
    # test_all_steam_apps_path = 1
    # test_date_path_part = '2222'

    # Server status test:
    steam_server_status()

    def setUp(self):
        AllSteamProductsData.all_steam_products_data_path = Parameter(self.test_all_steam_apps_path)
        AllSteamProductsData.date_path_part = DateParameter(default=self.test_date_path_part)
        self.AllSteamAppsData = AllSteamProductsData()

    def tearDown(self):
        pass

    @patch('steam_statistics_luigi_ETL.my_beautiful_task_data_landing',
           return_value=None)
    def test_AllSteamProductsData_task(self, mock_my_beautiful_task_data_landing):
        """
        The operability of the AllSteamAppsData task itself, without writing to disk.
        """
        partition_path = AllSteamProductsData.all_steam_products_data_path
        day_for_landing = str(self.test_date_path_part)
        mock_my_beautiful_task_data_landing.return_value = f'{partition_path}/{day_for_landing}/{"_Validate_Success"}'

        is_there_an_error = AllSteamProductsData().set_status_message
        self.assertEqual(is_there_an_error, None)
        warn = 0
        if type(self.test_all_steam_apps_path) is not str:
            warn = 1
        if '/' not in self.test_all_steam_apps_path:
            warn = 1
        self.assertEqual(warn, 0)

    @patch('steam_statistics_luigi_ETL.my_beautiful_task_data_landing',
           return_value=None)
    def test_AllSteamProductsData_run(self, mock_my_beautiful_task_data_landing):
        """
        Separate health check of the AllSteamAppsData.run module.
        """
        is_there_an_error = AllSteamProductsData.run(self=self.AllSteamAppsData)
        self.assertRaises(TypeError, is_there_an_error)


class TestGetSteamProductsDataInfo(unittest.TestCase):
    """
    Test GetSteamProductsDataInfo.
    """
    # Parameters without errors:
    test_get_steam_app_info_path = '~/Steam_ETL/Info_about_steam_apps'
    test_date_path_part = date(2012, 12, 15)
    test_df = DataFrame.from_dict({'app_id': {0: 220}, 'app_name': {0: 'Half-Life 2'}})

    # Parameters without errors:
    # test_get_steam_app_info_path = 1
    # test_date_path_part = '2222'
    # test_df = 1
    # test_df = DataFrame()

    def setUp(self):
        GetSteamProductsDataInfo.get_steam_products_data_info_path = Parameter(default='/dev/null')
        GetSteamProductsDataInfo.date_path_part = DateParameter(default=self.test_date_path_part)
        GetSteamProductsDataInfo.get_steam_products_data_info_loglevel = Parameter(default="0")
        GetSteamProductsDataInfo.get_steam_products_data_info_logfile_path = Parameter(default='/dev/null')
        GetSteamProductsDataInfo.apps_df_save_path = '/dev/null'
        GetSteamProductsDataInfo.dlc_df_save_path = '/dev/null'
        GetSteamProductsDataInfo.safe_dict_data_path = '/dev/null'
        GetSteamProductsDataInfo.safe_dict_dlc_data_path = '/dev/null'
        self.GetSteamAppInfo = GetSteamProductsDataInfo()

    @patch('Steam_statistics_tasks.GetSteamProductsDataInfo_steam_statistics_luigi_task.safe_dlc_data',
           return_value=test_df)
    @patch('steam_statistics_luigi_ETL.my_beautiful_task_data_landing',
           return_value=None)
    @patch('steam_statistics_luigi_ETL.GetSteamProductsDataInfo',
           interested_data=test_df)
    @patch('Steam_statistics_tasks.GetSteamProductsDataInfo_steam_statistics_luigi_task.safe_dict_data',
           return_value=None)
    def test_GetSteamProductsDataInfo_task(self, mock_safe_dict_data, mock_interested_data,
                                           mock_my_beautiful_task_data_landing, mock_safe_dlc_data):
        """
        The operability of the GetSteamAppInfo task itself, without writing to disk.
        """
        partition_path = GetSteamProductsDataInfo.get_steam_products_data_info_path
        day_for_landing = str(self.test_date_path_part)
        mock_my_beautiful_task_data_landing.return_value = f'{partition_path}/{day_for_landing}/{"_Validate_Success"}'

        is_there_an_error = GetSteamProductsDataInfo().set_status_message
        self.assertEqual(is_there_an_error, None)
        if type(self.test_get_steam_app_info_path) is not str:
            warn = 1
            self.assertEqual(
                warn, 0, '\nget_steam_app_info_path argument is not string type, it means that this is not path...')
        if '/' not in self.test_get_steam_app_info_path:
            warn = 1
            self.assertEqual(
                warn, 0, '\nget_steam_app_info_path argument have no "/" it means that this is not path...')

    @patch('Steam_statistics_tasks.GetSteamProductsDataInfo_steam_statistics_luigi_task.make_flag',
           flag_path='/dev/null')
    @patch('Steam_statistics_tasks.GetSteamProductsDataInfo_steam_statistics_luigi_task.make_flag',
           output_path='/dev/null')
    @patch('Steam_statistics_tasks.GetSteamProductsDataInfo_steam_statistics_luigi_task.safe_dict_data',
           return_value=None)
    @patch('steam_statistics_luigi_ETL.GetSteamProductsDataInfo',
           interested_data=test_df)
    def test_GetSteamProductsDataInfo_run(self, mock_interested_data, mock_safe_dict_data,
                                          mock_output_path, mock_flag_path):
        """
        Separate health check of the GetSteamAppInfo.run module.
        """
        is_there_an_error = GetSteamProductsDataInfo.run(self=self.GetSteamAppInfo)
        self.assertRaises(TypeError, is_there_an_error)
        if type(self.test_df) is not DataFrame:
            warn = 1
            self.assertEqual(warn, 0, f"'\nmock_interested_data.return_value is not Pandas.DataFrame(data)...'")
        if len(self.test_df) == 0:
            warn = 1
            self.assertEqual(warn, 0, '\nlen(mock_interested_data.return_value) == 0...')


class TestSteamAppsInfo(unittest.TestCase):
    """
    Test SteamAppsInfo.
    """
    # Parameters without errors:
    test_get_steam_app_info_path = '~/Steam_ETL/Info_about_steam_apps'
    test_date_path_part = date(2012, 12, 15)
    test_df = DataFrame.from_dict({'app_id': {0: 220}, 'app_name': {0: 'Half-Life 2'}})

    # Parameters without errors:
    # test_get_steam_app_info_path = 1
    # test_date_path_part = '2222'
    # test_df = 1
    # test_df = DataFrame()

    @patch('steam_statistics_luigi_ETL.my_beautiful_task_data_landing',
           return_value=None)
    def test_SteamAppsInfo_task(self, mock_my_beautiful_task_data_landing):
        """
        The operability of the GetSteamAppInfo task itself, without writing to disk.
        """
        SteamAppsInfo.steam_apps_info_path = Parameter(self.test_get_steam_app_info_path)
        SteamAppsInfo.date_path_part = DateParameter(default=self.test_date_path_part)
        self.GetSteamAppInfo = SteamAppsInfo()
        partition_path = SteamAppsInfo.steam_apps_info_path
        day_for_landing = str(self.test_date_path_part)
        mock_my_beautiful_task_data_landing.return_value = f'{partition_path}/{day_for_landing}/{"_Validate_Success"}'

        is_there_an_error = SteamAppsInfo().set_status_message
        self.assertEqual(is_there_an_error, None)
        if type(self.test_get_steam_app_info_path) is not str:
            warn = 1
            self.assertEqual(
                warn, 0, '\nget_steam_app_info_path argument is not string type, it means that this is not path...')
        if '/' not in self.test_get_steam_app_info_path:
            warn = 1
            self.assertEqual(
                warn, 0, '\nget_steam_app_info_path argument have no "/" it means that this is not path...')


if __name__ == '__main__':
    unittest.main()
