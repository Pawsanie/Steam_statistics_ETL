import json
from os import walk, path, makedirs
from pathlib import PurePath
from datetime import date

from luigi import Task, Parameter, DateParameter, LocalTarget
from pandas import DataFrame, read_csv, read_json
from pyarrow import Table, parquet
"""
Contains, in one way or another, a universal code for all 'steam statistics pipeline'.
"""


class DataFramesMerge:
    """
    Merges the given dataframes into one, filling NaN empty cells.
    """
    def data_frames_merge(self, *, data_from_files: DataFrame or None,
                          extracted_data: DataFrame) -> DataFrame:
        """
        Merges the given dataframes into one, filling NaN empty cells.
        """
        if data_from_files is None:
            data_from_files: DataFrame = extracted_data.astype(str)
        else:
            extract_data: DataFrame = extracted_data.astype(str)
            data_from_files = data_from_files.merge(extract_data, how='outer').reset_index(drop=True).astype(str)
        return data_from_files


class ExtractDataFromWarHouse(DataFramesMerge):
    """
    Retrieves data from the corresponding iteration of the previous Luigi task.

    :param result_successor: List, tuple or string with file`s paths.
    :type result_successor: list | tuple | str
    :param file_mask: Interesting file format.
    :type file_mask: str
    :param success_flag: String with name of success flag file.
    :type success_flag: str
    """

    dir_list: list[str] = []
    interested_partition: dict[str] = {}
    interested_data: dict[str, DataFrame] = {}

    def __init__(self, *, result_successor: list or tuple or str, file_mask: str, success_flag):
        """
        :param result_successor: List, tuple or string with file`s paths.
        :type result_successor: list | tuple | str
        :param file_mask: Interesting file format.
        :type file_mask: str
        :param success_flag: String with name of success flag file.
        :type success_flag: str
        """
        self.result_successor: list or tuple or str = result_successor
        self.file_mask: str = file_mask
        self.success_flag: str = success_flag

    def task_path_parser(self):
        """
        Inheritance of paths from result_successor.
        """
        if type(self.result_successor) is list or type(self.result_successor) is tuple:
            for flag in self.result_successor:
                if type(flag) is str:
                    path_to_table: str = str.replace(flag, self.success_flag, '')
                    self.dir_list.append(path_to_table)
                else:
                    path_to_table: str = str.replace(flag.path, self.success_flag, '')
                    self.dir_list.append(path_to_table)
        elif type(self.result_successor) is str:
            path_to_table: str = str.replace(self.result_successor, self.success_flag, '')
            self.dir_list.append(path_to_table)
        else:
            path_to_table: str = str.replace(self.result_successor.path, self.success_flag, '')
            self.dir_list.append(path_to_table)
        for parsing_dir in self.dir_list:
            for dirs, folders, files in walk(parsing_dir):
                for file in files:
                    partition_path: str = path.join(*[dirs, file])
                    if path.isfile(partition_path) and self.file_mask in file:
                        partition_path_split: tuple[str] = PurePath(partition_path).parts
                        partition_file: str = partition_path_split[-1]
                        partition_date: str = path.join(
                            *[partition_path_split[-4],
                              partition_path_split[-3],
                              partition_path_split[-2]])
                        partition_path: str = str.replace(
                            partition_path, path.join(*[partition_date, partition_file]), '')
                        interested_partition_path: str = path.join(
                            *[partition_path,
                              partition_date,
                              partition_file])
                        # Result:
                        self.interested_partition.setdefault(partition_date, {}) \
                            .update({partition_file: interested_partition_path})

    def task_universal_parser_part(self) -> dict[str, DataFrame]:
        """
        Runs code after inheriting paths from the previous task.
        """
        # Gets the data`s path for the current iteration:
        self.task_path_parser()
        # Parsing data from files along the paths inherited from the previous task:
        self.task_table_data_parser()
        return self.interested_data

    def how_to_extract(self, file: str) -> DataFrame:
        """
        Defines a pandas read method.
        :param file: Path to file for file read.
        :type file: str
        """
        if self.file_mask == 'csv':
            return read_csv(file).astype(str)
        if self.file_mask == 'json':
            return read_json(file, dtype='int64')

    def task_table_data_parser(self):
        """
        Universal reading of data from tables.
        """
        for key in self.interested_partition:
            data_from_files: None = None
            files = self.interested_partition.get(key).values()
            for file in files:  # Parsing tables in to raw DataFrame.
                extracted_data: DataFrame = self.how_to_extract(file)
                # Merging Pandas DataFrames
                data_from_files: DataFrame = self.data_frames_merge(
                    data_from_files=data_from_files,
                    extracted_data=extracted_data)
            self.interested_data[key]: dict[str, DataFrame] = data_from_files


class UniversalLuigiTask(Task, ExtractDataFromWarHouse):
    """
    Universal super class for Steam Statistics ETL Task.
    """
    landing_path_part: str = Parameter(
        significant=True,
        description='Root path for landing task result.')
    file_mask: str = Parameter(
        significant=True,
        description='File format for landing.')
    ancestor_file_mask: str = Parameter(
        significant=True,
        description='File format for extract.')
    date_path_part: date = DateParameter(
        significant=True,
        default=date.today(),
        description='Date for root path')
    file_name: str = Parameter(
        significant=True,
        default='',
        description='File name for landing.')
    # Task settings:
    success_flag: str = '_Validate_Success'
    output_path: str = ''  # Must be rewrite in "run()" method.

    def get_extract_data(self, requires) -> dict[str, DataFrame]:
        """
        Retrieves data from the corresponding iteration of the previous Luigi task.
        """
        extract_data = ExtractDataFromWarHouse(
            result_successor=requires,
            file_mask=str(self.ancestor_file_mask),
            success_flag=self.success_flag)
        return extract_data.task_universal_parser_part()

    def get_date_path_part(self) -> str:
        """
        Make string with YYYY-mm-dd date for output path.
        As example YYYY/mm/dd.
        """
        return path.join(*[str(self.date_path_part.year), str(self.date_path_part.month), str(self.date_path_part.day)])

    def output(self):
        date_path_part: str = self.get_date_path_part()
        self.output_path: str = path.join(*[str(self.landing_path_part), date_path_part])
        return LocalTarget(path.join(*[self.output_path, self.success_flag]))

    def task_data_landing(self, *, data_to_landing: dict or DataFrame,
                          output_path: str or None = None, file_name: str or None = None):
        """
        Landing parsed data as json, csv, or parquet.
        """
        # Arguments parsing:
        if file_name is None:
            file_name: str = self.file_name
        if output_path is None:
            output_path: str = self.output_path
        # Landing:
        data_type_need: str = f"{self.file_mask}"
        data_from_files: DataFrame = DataFrame(data_to_landing)
        if not path.exists(output_path):
            makedirs(output_path)
        flag_path: str = path.join(*[output_path, self.success_flag])
        output_path: str = path.join(*[output_path, f"{file_name}.{self.file_mask}"])
        if data_type_need == 'json':
            data_from_files: str = data_from_files.to_json(orient='records')
            data_from_files: str = json.loads(data_from_files)
            json_data: str = json.dumps(data_from_files, indent=4, ensure_ascii=False)
            with open(output_path, 'w', encoding='utf-8') as json_file:
                json_file.write(json_data)
        if data_type_need == 'parquet':
            parquet_table = Table.from_pandas(data_to_landing)
            parquet.write_table(parquet_table, output_path, use_dictionary=False, compression=None)
        if data_type_need == 'csv':
            data_to_csv: str = data_to_landing.to_csv(index=False)
            with open(output_path, 'w') as csv_file:
                csv_file.write(data_to_csv)
        with open(flag_path, 'w'):
            pass
