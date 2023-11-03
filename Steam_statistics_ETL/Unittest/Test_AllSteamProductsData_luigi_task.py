from unittest.mock import patch

from requests import get

from .Test_Universal_luigi_task import TestParameters
from ..Steam_statistics_tasks.AllSteamProductsData_luigi_task import AllSteamProductsDataTask
"""
Contains AllSteamProductsData unittests.
"""


class TestAllSteamProductsData(TestParameters):
    """
    Test AllSteamProductsData.
    """
    ancestor_file_mask: str = 'json'
    file_mask: str = 'json'
    file_name: str = 'AllSteamProductsData'
    # Task settings:
    task_class_name = AllSteamProductsDataTask

    def steam_server_status(self):
        """
        Server status test.
        """
        server_satus = get('http://api.steampowered.com/ISteamWebAPIUtil/GetServerInfo/v0001/')
        print('\nSteam WEB-API server satus: ' + str(server_satus))
        self.assertEqual(str(server_satus), '<Response [200]>')

    def AllSteamProductsData_task(self):
        """
        The operability of the AllSteamAppsData task itself, without writing to disk.
        """
        with patch.object(
                AllSteamProductsDataTask,
                'task_data_landing',
                return_value=None
        ):
            is_there_an_error = self.task_class_name.set_status_message
            self.assertEqual(is_there_an_error, None)

    def AllSteamProductsData_run(self):
        """
        Separate health check of the 'AllSteamAppsData.run' module.
        """
        # self.prepare_data_landing_for_testing()
        with patch.object(AllSteamProductsDataTask,
                          'task_data_landing',
                          return_value=None
                          # new=Patch_Module.task_data_landing(
                          #     self=self.task_class_name,
                          #     data_to_landing=self.get_test_data_frame(),
                          #     output_path=self.get_test_path(),
                          #     file_name='null'
                          #     )
                          ):
            is_there_an_error = self.task_class_name.run()
            self.assertRaises(TypeError, is_there_an_error)
