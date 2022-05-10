import unittest
from unittest.mock import patch, Mock
from requests import get
import luigi.notifications
from luigi import DateParameter, Parameter
from datetime import date
from os.path import expanduser
from pandas import DataFrame
from steam_statistics_luigi_ETL import AllSteamAppsData, GetSteamAppInfo
from steam_statistics_luigi_tasks import *

luigi.notifications.DEBUG = True
home = expanduser("~")


def steam_server_status():
    """
    Тест статуса сервера.
    """
    server_satus = get('http://api.steampowered.com/ISteamWebAPIUtil/GetServerInfo/v0001/')
    print('\nSteam WEB-API server satus: ' + str(server_satus))


class TestAllSteamAppsData(unittest.TestCase):
    """
    Тест AllSteamAppsData.
    """
    #  Параметры без ошибок:
    test_all_steam_apps_path = '~/Steam_ETL/All_steam_apps'
    test_date_path_part = date(2012, 12, 15)
    # Параметры с ошибками:
    # test_all_steam_apps_path = 1
    # test_date_path_part = '2222'

    def setUp(self):
        steam_server_status()

    def tearDown(self):
        pass

    @patch('steam_statistics_luigi_ETL.my_beautiful_task_data_landing',
           my_beautiful_task_data_landing=my_beautiful_task_data_landing)
    def test_task(self, mock_my_beautiful_task_data_landing):
        """
        Работоспособность самой AllSteamAppsData таски, без записи на диск.
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
           my_beautiful_task_data_landing=my_beautiful_task_data_landing)
    def test_run(self, mock_my_beautiful_task_data_landing):
        """
        Отдельная проверка работоспособности модуля AllSteamAppsData.run.
        """
        AllSteamAppsData.all_steam_apps_path = Parameter(self.test_all_steam_apps_path)
        AllSteamAppsData.date_path_part = DateParameter(default=self.test_date_path_part)
        self.AllSteamAppsData = AllSteamAppsData()
        is_there_an_error = AllSteamAppsData.run(self=self.AllSteamAppsData)
        self.assertRaises(TypeError, is_there_an_error)


class TestGetSteamAppInfo(unittest.TestCase):
    """
    Тест GetSteamAppInfo.
    """
    #  Параметры без ошибок:
    test_get_steam_app_info_path = '~/Steam_ETL/Info_about_steam_apps'
    test_date_path_part = date(2012, 12, 15)
    test_df = DataFrame.from_dict({'app_id': {0: 220}, 'app_name': {0: 'Half-Life 2'}})
    # Параметры с ошибками:
    # test_get_steam_app_info_path = 1
    # test_date_path_part = '2222'
    # test_df = 1
    # test_df = DataFrame()

    @patch('steam_statistics_luigi_ETL.my_beautiful_task_data_landing',
           my_beautiful_task_data_landing=my_beautiful_task_data_landing)
    @patch('steam_statistics_luigi_ETL.GetSteamAppInfo',
           interested_data=test_df)
    @patch('steam_statistics_luigi_tasks.safe_dict_data',
           safe_dict_data=safe_dict_data)
    def test_task(self, mock_my_beautiful_task_data_landing, mock_interested_data, mock_safe_dict_data):
        """
        Работоспособность самой GetSteamAppInfo таски, без записи на диск.
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
            self.assertEqual(warn, 0, '\nget_steam_app_info_path argument is not string type, it means that this is not path...')
        if '/' not in self.test_get_steam_app_info_path:
            warn = 1
            self.assertEqual(warn, 0, '\nget_steam_app_info_path argument have no "/" it means that this is not path...')

    @patch('steam_statistics_luigi_ETL.my_beautiful_task_data_landing',
           my_beautiful_task_data_landing=my_beautiful_task_data_landing)
    @patch('steam_statistics_luigi_ETL.GetSteamAppInfo.run',
           interested_data=None)
    @patch('steam_statistics_luigi_tasks.safe_dict_data',
           steam_apps_parser=steam_apps_parser)
    def test_run(self, mock_my_beautiful_task_data_landing, mock_steam_apps_parser, mock_safe_dict_data):
        """
        Отдельная проверка работоспособности модуля GetSteamAppInfo.run.
        """
        GetSteamAppInfo.get_steam_app_info_path = Parameter(self.test_get_steam_app_info_path)
        GetSteamAppInfo.date_path_part = DateParameter(default=self.test_date_path_part)
        mock_steam_apps_parser.return_value = self.test_df
        self.GetSteamAppInfo = GetSteamAppInfo()
        is_there_an_error = GetSteamAppInfo.run(self=self.GetSteamAppInfo)
        self.assertRaises(TypeError, is_there_an_error)
        warn = 0
        if type(mock_steam_apps_parser.return_value) is not DataFrame:
            warn = 1
            self.assertEqual(warn, 0, '\nmock_steam_apps_parser.return_value is not Pandas.DataFrame(data)...')
        if len(mock_steam_apps_parser.return_value) == 0:
            warn = 1
            self.assertEqual(warn, 0, '\nlen(mock_steam_apps_parser.return_value) == 0...')


if __name__ == '__main__':
    unittest.main()
