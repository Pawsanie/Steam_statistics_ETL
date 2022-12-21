from datetime import date
from os import devnull, path
import unittest
from pathlib import PurePath

from pandas import DataFrame
from luigi import LocalTarget
"""
Contains arguments settings for unittests.
"""


def test_path_value(*args) -> str or int:
    """
    Test path value.
    """
    self = args[0]
    if self.parameters_errors_status is False:
        # Parameters without errors:
        return devnull
    else:
        # Parameters with errors:
        return 1


def test_date_value(*args) -> date or str:
    """
    Test date value.
    """
    self = args[0]
    if self.parameters_errors_status is False:
        # Parameters without errors:
        return date(2012, 12, 15)
    else:
        # Parameters with errors:
        return '2222'


def test_data_frame_value(*args) -> DataFrame:
    """
    Test pandas.DataFrame value.
    """
    self = args[0]
    if self.parameters_errors_status is False:
        # Parameters without errors:
        return DataFrame.from_dict(
            {'app_id': {0: 220}, 'app_name': {0: 'Test-Game2'}}
        )
    else:
        # Parameters with errors:
        return DataFrame()


def test_all_apps_data_frame_value(*args) -> DataFrame:
    """
    Test pandas.DataFrame value.
    """
    self = args[0]
    if self.parameters_errors_status is False:
        # Parameters without errors:
        return DataFrame.from_dict(
                    {
                        'appid': {0: 220}, 'name': {0: 'Test-Game2'}
                    }
                )
    else:
        # Parameters with errors:
        return DataFrame()


def test_file_type(*args, file_type: str) -> str or int:
    """
    Test file type value.
    """
    self = args[0]
    if self.parameters_errors_status is False:
        # Parameters without errors:
        return file_type
    else:
        # Parameters with errors:
        return 1


class TestParameters(unittest.TestCase):
    """
    Super class for pipeline unittests.
    """
    # Parameters status:
    parameters_errors_status: bool = False
    # Parameters without errors:
    # test_df: DataFrame = test_data_frame_value()
    ancestor_file_mask: str = ''
    file_mask: str = ''
    file_name: str = ''
    success_flag: str = '_Validate_Success'
    # Task settings:
    task_class_name = ...  # Class must be rewritten in a children.
    # Assert settings:
    error_count: int = 0

    def setUp(self):
        # Luigi parameters:
        self.task_class_name.landing_path_part = test_path_value(self)
        self.task_class_name.file_mask = test_file_type(self, file_type=self.file_mask)
        self.task_class_name.ancestor_file_mask = test_file_type(self, file_type=self.file_mask)
        self.task_class_name.date_path_part = test_date_value(self)
        self.task_class_name.file_name = self.file_name
        self.task_class_name.success_flag = self.success_flag
        self.task_class_name.output_path = test_path_value(self)
        self.task_class_name.output = LocalTarget(
            path.join(*[
                self.task_class_name.output_path,
                self.success_flag
            ]))
        self.task_class_name.requires = ...
        self.task_class_name.logfile_path = test_path_value(self)
        # Class exemplar:
        self.task_class_name = self.task_class_name()

    def tearDown(self):
        pass

    def get_test_path(self) -> str:
        test_path_list: tuple[str] = PurePath(test_path_value(self)).parts
        return test_path_value(self).replace(test_path_list[-1], '')

    def get_test_data_frame(self) -> DataFrame:
        return test_data_frame_value(self)

    def get_date_path(self) -> str:
        date_path_part = test_date_value(self)
        return path.join(*[str(date_path_part.year), str(date_path_part.month), str(date_path_part.day)])

    def get_json_dataframe(self) -> DataFrame:
        return test_all_apps_data_frame_value(self)

    def prepare_data_landing_for_testing(self):
        #  Class exemplar data landing:
        self.task_class_name.file_mask = ''
        self.task_class_name.success_flag = 'null'
