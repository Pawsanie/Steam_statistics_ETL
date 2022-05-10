import unittest
from unittest.mock import patch, Mock
from requests import get
import luigi.notifications
from luigi import DateParameter, Parameter
from datetime import date
from os.path import expanduser
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
    test_all_steam_apps_path = '~/Steam_ETL/All_steam_apps'
    test_date_path_part = date(2012, 12, 15)

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @patch('steam_statistics_luigi_ETL.my_beautiful_task_data_landing',
           my_beautiful_task_data_landing=my_beautiful_task_data_landing)
    def test_task(self, mock_my_beautiful_task_data_landing):
        steam_server_status()
        AllSteamAppsData.all_steam_apps_path = Parameter(self.test_all_steam_apps_path)
        AllSteamAppsData.date_path_part = DateParameter(default=self.test_date_path_part)
        is_there_an_error_error = AllSteamAppsData().set_status_message
        self.assertEqual(is_there_an_error_error, None)

    @patch('steam_statistics_luigi_ETL.my_beautiful_task_data_landing',
           my_beautiful_task_data_landing=my_beautiful_task_data_landing)
    def test_run(self, mock_my_beautiful_task_data_landing):
        AllSteamAppsData.all_steam_apps_path = Parameter(self.test_all_steam_apps_path)
        AllSteamAppsData.date_path_part = DateParameter(default=self.test_date_path_part)
        self.AllSteamAppsData = AllSteamAppsData()
        AllSteamAppsData.run(self=self.AllSteamAppsData)

    @patch('steam_statistics_luigi_ETL.my_beautiful_task_data_landing',
           my_beautiful_task_data_landing=my_beautiful_task_data_landing)
    def test_output(self, mock_my_beautiful_task_data_landing):
        AllSteamAppsData.all_steam_apps_path = Parameter(self.test_all_steam_apps_path)
        AllSteamAppsData.date_path_part = DateParameter(default=self.test_date_path_part)
        partition_path = AllSteamAppsData.all_steam_apps_path
        day_for_landing = str(self.test_date_path_part)
        mock_my_beautiful_task_data_landing.return_value = f'{partition_path}/{day_for_landing}/{"_Validate_Success"}'


if __name__ == '__main__':
    unittest.main()
