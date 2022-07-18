import json
from pandas import DataFrame, read_csv, read_json
from os import walk, path, makedirs
from pyarrow import Table, parquet
"""
Contains, in one way or another, a universal code for all 'steam statistics pipeline'.
"""


def my_beautiful_task_data_landing(data_to_landing: dict or DataFrame, day_for_landing: str,
                                   partition_path: str, file_mask: str) -> str:
    """
    Landing parsed data as json, csv, or parquet.
    '''
    Приземление распаршеных данных в виде json, csv, или parquet.
    """
    data_type_need = file_mask.split('.')
    data_type_need = data_type_need[1]
    output_path = f'{partition_path}/{day_for_landing}'
    data_from_files = DataFrame(data_to_landing)
    if not path.exists(output_path):
        makedirs(output_path)
    flag_path = f'{output_path}/{"_Validate_Success"}'
    output_path = f'{output_path}/{file_mask}'
    if data_type_need == 'json':
        data_from_files = data_from_files.to_json(orient='records')
        data_from_files = json.loads(data_from_files)
        json_data = json.dumps(data_from_files, indent=4, ensure_ascii=False)
        with open(output_path, 'w', encoding='utf-8') as json_file:
            json_file.write(json_data)
    if data_type_need == 'parquet':
        parquet_table = Table.from_pandas(data_to_landing)
        parquet.write_table(parquet_table, output_path, use_dictionary=False, compression=None)
    if data_type_need == 'csv':
        data_to_csv = data_to_landing.to_csv(index=False)
        with open(output_path, 'w') as csv_file:
            csv_file.write(data_to_csv)
    flag = open(flag_path, 'w')
    flag.close()
    return flag_path


def my_beautiful_task_path_parser(result_successor: list or tuple or str, dir_list: list,
                                  interested_partition: dict, file_mask: str):
    """
    Inheritance of paths from result_successor.
    '''
    Наследование путей из result_successor.
    """
    if type(result_successor) is list or type(result_successor) is tuple:
        for flag in result_successor:
            if type(flag) is str:
                path_to_table = str.replace(flag, '_Validate_Success', '')
                dir_list.append(path_to_table)
            else:
                path_to_table = str.replace(flag.path, '_Validate_Success', '')
                dir_list.append(path_to_table)
    elif type(result_successor) is str:
        path_to_table = str.replace(result_successor, '_Validate_Success', '')
        dir_list.append(path_to_table)
    else:
        path_to_table = str.replace(result_successor.path, '_Validate_Success', '')
        dir_list.append(path_to_table)
    for parsing_dir in dir_list:
        for dirs, folders, files in walk(parsing_dir):
            for file in files:
                partition_path = f'{dirs}{file}'
                if path.isfile(partition_path) and file_mask in file:
                    partition_path_split = partition_path.split('/')
                    partition_file = partition_path_split[-1]
                    partition_date = f'{partition_path_split[-4]}/{partition_path_split[-3]}' \
                                     f'/{partition_path_split[-2]}/'
                    partition_path = str.replace(partition_path, partition_date + partition_file, '')
                    interested_partition_path = f'{partition_path}{partition_date}{partition_file}'
                    interested_partition.setdefault(partition_date, {}).update(
                        {partition_file: interested_partition_path})


def my_beautiful_task_data_frame_merge(data_from_files: DataFrame or None, extract_data: DataFrame) -> DataFrame:
    """
    Merges the given dataframes into one, filling NaN empty cells.
    '''
    Объединяет переданные датафреймы в один, заполняя  NaN пустые ячейки.
    """
    if data_from_files is None:
        data_from_files = extract_data
    else:
        extract_data = extract_data.astype(object)
        data_from_files = data_from_files.merge(extract_data, how='outer')
        data_from_files = data_from_files.reset_index(drop=True)
    return data_from_files


def my_beautiful_task_data_table_parser(interested_partition: dict, drop_list: list or None,
                                        interested_data, file_mask: str):
    """
    Universal reading of data from tables.
    '''
    Универсальное чтение данных из таблиц
    """
    def how_to_extract(*args):  # Определение метода чтения данных для pandas.
        how_to_extract_format = None
        if file_mask == '.csv':
            how_to_extract_format = read_csv(*args).astype(str)
        if file_mask == '.json':
            how_to_extract_format = read_json(*args, dtype='int64')
            # Json требует ручного указания типа вывода для длинных чисел
        return how_to_extract_format

    for key in interested_partition:
        data_from_files = None
        files = interested_partition.get(key)
        files = files.values()
        for file in files:  # Парсинг таблиц в сырой датафрейм
            if drop_list is not None:
                extract_data = how_to_extract(file).drop([drop_list], axis=1)
            else:
                extract_data = how_to_extract(file)
            data_from_files = my_beautiful_task_data_frame_merge(data_from_files, extract_data)  # Слияние датафреймов
        interested_data[key] = data_from_files


def my_beautiful_task_universal_parser_part(result_successor: list or tuple or str,
                                            file_mask: str, drop_list: list or None) -> dict:
    """
    Runs code after inheriting paths from the previous task.
    '''
    Запускает код после наследования путей от прошлой таски.
    """
    interested_partition, dir_list = {}, []
    my_beautiful_task_path_parser(result_successor, dir_list, interested_partition, file_mask)

    interested_data = {}  # Парсинг данных из файлов по путям унаследованным от прошлой таски.
    my_beautiful_task_data_table_parser(interested_partition, drop_list, interested_data, file_mask)
    return interested_data
