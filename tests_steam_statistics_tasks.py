import unittest

import luigi.notifications

from Steam_statistics_ETL.Unittest.Test_AllSteamProductsData_luigi_task import TestAllSteamProductsData
from Steam_statistics_ETL.Unittest.Test_GetSteamProductsDataInfo_luigi_task import TestGetSteamProductsDataInfo
from Steam_statistics_ETL.Unittest.Test_SteamProductsInfoCSVJoiner_luigi_task import TestSteamProductsInfoCSVJoiner
"""
Steam statistics Luigi ETL unit tests.
"""

luigi.notifications.DEBUG = True


class TestAllSteamProductsDataTask(TestAllSteamProductsData):
    """
    Test AllSteamProductsData.
    """
    # Parameters status:
    parameters_errors_status: bool = False

    def test_steam_server_status(self):
        super(TestAllSteamProductsDataTask, self).steam_server_status()

    def test_AllSteamProductsData_task(self):
        super(TestAllSteamProductsDataTask, self).AllSteamProductsData_task()

    def test_AllSteamProductsData_run(self):
        super(TestAllSteamProductsDataTask, self).AllSteamProductsData_run()


class TestGetSteamProductsDataInfoTask(TestGetSteamProductsDataInfo):
    """
    Test GetSteamProductsDataInfo.
    """
    # Parameters status:
    parameters_errors_status: bool = False

    def test_GetSteamProductsDataInfo_task(self):
        super(TestGetSteamProductsDataInfoTask, self).GetSteamProductsDataInfo_task()

    def test_GetSteamProductsDataInfo_run(self):
        super(TestGetSteamProductsDataInfoTask, self).GetSteamProductsDataInfo_run()


class TestSteamProductsInfoCSVJoinerTask(TestSteamProductsInfoCSVJoiner):
    """
    Test SteamProductsInfoCSVJoiner.
    """
    # Parameters status:
    parameters_errors_status: bool = False

    def test_GetSteamProductsDataInfo_task(self):
        super(TestSteamProductsInfoCSVJoinerTask, self).SteamProductsInfoInfoCSVJoiner_task()

    def test_SteamProductsInfoInfoCSVJoiner_run(self):
        super(TestSteamProductsInfoCSVJoinerTask, self).SteamProductsInfoInfoCSVJoiner_run()


if __name__ == '__main__':
    unittest.main()
