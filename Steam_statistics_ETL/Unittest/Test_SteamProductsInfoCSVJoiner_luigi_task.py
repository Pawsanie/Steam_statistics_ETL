from unittest.mock import patch, MagicMock

from pandas import DataFrame

from .Test_Universal_luigi_task import TestParameters
from ..Steam_statistics_tasks.SteamProductsInfoCSVJoiner_luigi_task import SteamProductsInfoInfoCSVJoinerTask
"""
Contains SteamProductsInfoInfoCSVJoiner unittests.
"""


class TestSteamProductsInfoCSVJoiner(TestParameters):
    """
    Test SteamProductsInfoInfoCSVJoiner.
    """
    ancestor_file_mask: str = 'csv'
    file_mask: str = 'csv'
    file_name: str = ''
    # Task settings:
    task_class_name = SteamProductsInfoInfoCSVJoinerTask

    def setUp(self):
        super(TestSteamProductsInfoCSVJoiner, self).setUp()
        self.task_class_name.directory_for_csv_join = 'Products_info'
        self.task_class_name.interested_data = {self.get_date_path(): DataFrame.from_dict(
            {'app_id': {0: 220}, 'app_name': {0: 'Test-Game2'}, 'tags': {0: 'test'}}
        )}

    def SteamProductsInfoInfoCSVJoiner_task(self):
        """
        The operability of the SteamProductsInfoInfoCSVJoiner task itself, without writing to disk.
        """
        with patch.object(
                SteamProductsInfoInfoCSVJoinerTask,
                'task_data_landing',
                return_value=None
        ):
            is_there_an_error = self.task_class_name.set_status_message
            self.assertEqual(is_there_an_error, None)

    def SteamProductsInfoInfoCSVJoiner_run(self):
        """
        Separate health check of the SteamProductsInfoInfoCSVJoiner.run module.
        """
        SteamProductsInfoInfoCSVJoinerTask.task_data_landing = MagicMock(
            return_value=None
        )
        SteamProductsInfoInfoCSVJoinerTask.input = MagicMock(
            return_value={'GetSteamProductsDataInfo': self.get_test_path()}
        )
        SteamProductsInfoInfoCSVJoinerTask.get_csv_for_join = MagicMock(
            return_value=None
        )

        is_there_an_error = self.task_class_name.run()
        self.assertRaises(TypeError, is_there_an_error)
