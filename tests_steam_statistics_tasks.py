import unittest
from unittest.mock import patch, Mock
from requests import get
import luigi.notifications
from luigi import DateParameter, Parameter
from datetime import date
from pandas import DataFrame
from steam_statistics_luigi_ETL import AllSteamAppsData, GetSteamAppInfo, AppInfoCSVJoiner
from Steam_statistics_tasks.Universal_steam_statistics_luigi_task import *
from Steam_statistics_tasks.GetSteamAppInfo_steam_statistics_luigi_task import *
from Steam_statistics_tasks.AppInfoCSVJoiner_steam_statistics_luigi_task import *


luigi.notifications.DEBUG = True


def steam_server_status():
    """
    Server status test.
    """
    server_satus = get('http://api.steampowered.com/ISteamWebAPIUtil/GetServerInfo/v0001/')
    print('\nSteam WEB-API server satus: ' + str(server_satus))


class TestAllSteamAppsData(unittest.TestCase):
    """
    Test AllSteamAppsData.
    """
    # Parameters without errors:
    test_all_steam_apps_path = '~/Steam_ETL/All_steam_apps'
    test_date_path_part = date(2012, 12, 15)
    # Parameters with errors:
    # test_all_steam_apps_path = 1
    # test_date_path_part = '2222'

    def setUp(self):
        steam_server_status()

    def tearDown(self):
        pass

    @patch('steam_statistics_luigi_ETL.my_beautiful_task_data_landing',
           return_value=None)
    def test_task(self, mock_my_beautiful_task_data_landing):
        """
        The operability of the AllSteamAppsData task itself, without writing to disk.
        """
        AllSteamAppsData.all_steam_apps_path = Parameter(self.test_all_steam_apps_path)
        AllSteamAppsData.date_path_part = DateParameter(default=self.test_date_path_part)
        self.AllSteamAppsData = AllSteamAppsData()
        partition_path = AllSteamAppsData.all_steam_apps_path
        day_for_landing = str(self.test_date_path_part)
        mock_my_beautiful_task_data_landing.return_value = f'{partition_path}/{day_for_landing}/{"_Validate_Success"}'
        is_there_an_error = AllSteamAppsData().set_status_message
        self.assertEqual(is_there_an_error, None)
        warn = 0
        if type(self.test_all_steam_apps_path) is not str:
            warn = 1
        if '/' not in self.test_all_steam_apps_path:
            warn = 1
        self.assertEqual(warn, 0)

    @patch('steam_statistics_luigi_ETL.my_beautiful_task_data_landing',
           return_value=None)
    def test_run(self, mock_my_beautiful_task_data_landing):
        """
        Separate health check of the AllSteamAppsData.run module.
        """
        AllSteamAppsData.all_steam_apps_path = Parameter(self.test_all_steam_apps_path)
        AllSteamAppsData.date_path_part = DateParameter(default=self.test_date_path_part)
        self.AllSteamAppsData = AllSteamAppsData()
        is_there_an_error = AllSteamAppsData.run(self=self.AllSteamAppsData)
        self.assertRaises(TypeError, is_there_an_error)


class TestGetSteamAppInfo(unittest.TestCase):
    """
    Test GetSteamAppInfo.
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
    @patch('steam_statistics_luigi_ETL.GetSteamAppInfo',
           interested_data=test_df)
    @patch('Steam_statistics_tasks.GetSteamAppInfo_steam_statistics_luigi_task.safe_dict_data',
           return_value=None)
    def test_task(self, mock_my_beautiful_task_data_landing, mock_interested_data, mock_safe_dict_data):
        """
        The operability of the GetSteamAppInfo task itself, without writing to disk.
        """
        GetSteamAppInfo.get_steam_app_info_path = Parameter(self.test_get_steam_app_info_path)
        GetSteamAppInfo.date_path_part = DateParameter(default=self.test_date_path_part)
        self.GetSteamAppInfo = GetSteamAppInfo()
        partition_path = GetSteamAppInfo.get_steam_app_info_path
        day_for_landing = str(self.test_date_path_part)
        mock_my_beautiful_task_data_landing.return_value = f'{partition_path}/{day_for_landing}/{"_Validate_Success"}'
        is_there_an_error = GetSteamAppInfo().set_status_message
        self.assertEqual(is_there_an_error, None)
        warn = 0
        if type(self.test_get_steam_app_info_path) is not str:
            warn = 1
            self.assertEqual(
                warn, 0, '\nget_steam_app_info_path argument is not string type, it means that this is not path...')
        if '/' not in self.test_get_steam_app_info_path:
            warn = 1
            self.assertEqual(
                warn, 0, '\nget_steam_app_info_path argument have no "/" it means that this is not path...')

    @patch('Steam_statistics_tasks.Universal_steam_statistics_luigi_task.my_beautiful_task_data_landing',
           return_value=None)
    @patch('Steam_statistics_tasks.GetSteamAppInfo_steam_statistics_luigi_task.safe_dict_data',
           return_value=None)
    def test_run(self, mock_my_beautiful_task_data_landing, mock_safe_dict_data):
        """
        Separate health check of the GetSteamAppInfo.run module.
        """
        GetSteamAppInfo.get_steam_app_info_path = Parameter(self.test_get_steam_app_info_path)
        GetSteamAppInfo.date_path_part = DateParameter(default=self.test_date_path_part)

        self.GetSteamAppInfo = GetSteamAppInfo()
        is_there_an_error = GetSteamAppInfo.run(self=self.GetSteamAppInfo)
        self.assertRaises(TypeError, is_there_an_error)
        # warn = 0
        # if type(mock_steam_apps_parser.return_value) is not DataFrame:
        #     warn = 1
        #     self.assertEqual(warn, 0, '\nmock_steam_apps_parser.return_value is not Pandas.DataFrame(data)...')
        # if len(mock_steam_apps_parser.return_value) == 0:
        #     warn = 1
        #     self.assertEqual(warn, 0, '\nlen(mock_steam_apps_parser.return_value) == 0...')


# class TestAppInfoCSVJoiner(unittest.TestCase):
#     """
#     Test AppInfoCSVJoiner.
#     """
#     # Parameters without errors:
#     test_get_steam_app_info_path = '~/Steam_ETL/Info_about_steam_apps'
#     test_date_path_part = date(2012, 12, 15)
#     test_df = DataFrame.from_dict({'app_id': {0: 220}, 'app_name': {0: 'Half-Life 2'}})
#
#     # Parameters without errors:
#     # test_get_steam_app_info_path = 1
#     # test_date_path_part = '2222'
#     # test_df = 1
#     # test_df = DataFrame()
#
#     @patch('Steam_statistics_tasks.Universal_steam_statistics_luigi_task.my_beautiful_task_data_landing',
#            my_beautiful_task_data_landing=my_beautiful_task_data_landing)
#     @patch('steam_statistics_luigi_ETL.AllSteamAppsData.interested_data',
#            return_value=test_df)
#     @patch('Steam_statistics_tasks.GetSteamAppInfo_steam_statistics_luigi_task.safe_dict_data',
#            safe_dict_data=safe_dict_data)
#     def test_task(self, mock_my_beautiful_task_data_landing, mock_interested_data, mock_safe_dict_data):
#         """
#         The operability of the GetSteamAppInfo task itself, without writing to disk.
#         """
#         GetSteamAppInfo.get_steam_app_info_path = Parameter(self.test_get_steam_app_info_path)
#         GetSteamAppInfo.date_path_part = DateParameter(default=self.test_date_path_part)
#         self.GetSteamAppInfo = GetSteamAppInfo()
#         partition_path = GetSteamAppInfo.get_steam_app_info_path
#         day_for_landing = str(self.test_date_path_part)
#         mock_my_beautiful_task_data_landing.return_value = f'{partition_path}/{day_for_landing}/{"_Validate_Success"}'
#         is_there_an_error = GetSteamAppInfo().set_status_message
#         self.assertEqual(is_there_an_error, None)
#         warn = 0
#         if type(self.test_get_steam_app_info_path) is not str:
#             warn = 1
#             self.assertEqual(
#                 warn, 0, '\nget_steam_app_info_path argument is not string type, it means that this is not path...')
#         if '/' not in self.test_get_steam_app_info_path:
#             warn = 1
#             self.assertEqual(
#                 warn, 0, '\nget_steam_app_info_path argument have no "/" it means that this is not path...')


if __name__ == '__main__':
    unittest.main()
