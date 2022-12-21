from unittest.mock import patch, MagicMock
from os import devnull

from pandas import DataFrame

from .Test_Universal_luigi_task import TestParameters
from ..Steam_statistics_tasks.GetSteamProductsDataInfo_luigi_task import GetSteamProductsDataInfoTask
"""
Contains GetSteamProductsDataInfo unittests.
"""


class TestGetSteamProductsDataInfo(TestParameters):
    """
    Test GetSteamProductsDataInfo.
    """
    ancestor_file_mask: str = 'json'
    file_mask: str = 'csv'
    file_name: str = ''
    # Task settings:
    task_class_name = GetSteamProductsDataInfoTask

    def setUp(self):
        super(TestGetSteamProductsDataInfo, self).setUp()
        self.task_class_name.apps_df_save_path = devnull
        self.task_class_name.dlc_df_save_path = devnull
        self.task_class_name.safe_dict_data_path = devnull
        self.task_class_name.safe_dict_dlc_data_path = devnull
        self.task_class_name.interested_data = DataFrame.from_dict(
                    {
                        'appid': {0: 220}, 'name': {0: 'Test-Game2'}
                    }
                )

    def GetSteamProductsDataInfo_task(self):
        """
        The operability of the GetSteamProductsDataInfo task itself, without writing to disk.
        """
        with patch.object(GetSteamProductsDataInfoTask,
                          'task_data_landing',
                          return_value=None):
            GetSteamProductsDataInfoTask.make_flag = MagicMock(
                return_value=devnull
            )
            is_there_an_error = self.task_class_name.set_status_message
            self.assertEqual(is_there_an_error, None)

    def GetSteamProductsDataInfo_run(self):
        """
        Separate health check of the GetSteamProductsDataInfo.run module.
        """
        GetSteamProductsDataInfoTask.task_data_landing = MagicMock(
            return_value=None
        )
        GetSteamProductsDataInfoTask.input = MagicMock(
            return_value={'AllSteamProductsData': self.get_test_path()}
        )
        GetSteamProductsDataInfoTask.get_extract_data = MagicMock(
            return_value={self.get_date_path(): self.get_json_dataframe()}
        )
        GetSteamProductsDataInfoTask.data_from_file_to_pd_dataframe = MagicMock(
            return_value=self.get_test_data_frame()
        )
        GetSteamProductsDataInfoTask.make_flag = MagicMock(
            return_value=devnull
        )

        is_there_an_error = self.task_class_name.run()
        self.assertRaises(TypeError, is_there_an_error)
