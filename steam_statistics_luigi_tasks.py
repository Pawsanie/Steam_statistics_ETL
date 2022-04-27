from os import walk, path, makedirs
import json
from pandas import DataFrame, read_csv, read_json
from numpy import NaN
import typing
from pyarrow import Table, parquet
from requests import get
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from datetime import datetime


def my_beautiful_task_data_landing(data_to_landing, day_for_landing, partition_path, file_mask):
    """Приземление распаршеных данных в виде json, или parquet."""
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
    flag = open(flag_path, 'w')
    flag.close()
    return flag_path


def my_beautiful_task_path_parser(result_successor, dir_list, interested_partition, file_mask):
    """Наследование путей из result_successor."""
    if result_successor is list or result_successor is tuple:
        for flag in result_successor:
            path_to_table = str.replace(flag.path, '_Validate_Success', '')
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


def my_beautiful_task_data_frame_merge(data_from_files, extract_data):
    """Объеденяет переданные датафреймы в один, заполняя  NaN пустые ячейки."""
    if data_from_files is None:
        data_from_files = extract_data
    else:
        new_point_for_merge = extract_data.columns.difference(data_from_files.columns)
        for column in new_point_for_merge:
            data_from_files.astype(object)[column] = NaN
        data_from_files = data_from_files.merge(extract_data, how='outer')
    return data_from_files


def my_beautiful_task_data_table_parser(interested_partition, drop_list, interested_data, file_mask):
    """Уневерсальное чтение данных из таблиц"""
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


def my_beautiful_task_universal_parser_part(result_successor, file_mask, drop_list):
    """Запускает код после наследования путей от прошлой таски."""
    interested_partition = {}
    dir_list = []
    my_beautiful_task_path_parser(result_successor, dir_list, interested_partition, file_mask)
    interested_data = {}  # Парсинг данных из файлов по путям унаследованным от прошлой таски.
    my_beautiful_task_data_table_parser(interested_partition, drop_list, interested_data, file_mask)
    return interested_data


def ask_app_in_steam_store(app_id, app_name):
    app_page_url = f"{'https://store.steampowered.com/app'}/{app_id}/{app_name}"
    app_page = get(app_page_url)
    soup = BeautifulSoup(app_page.text, "lxml")
    # ua = UserAgent(cache=False)
    # test = ua.random
    app_ratings = soup.find_all('span', class_='nonresponsive_hidden responsive_reviewdesc')
    result = {"rating_30d": {"%": "", "count": ""}, "rating_all_time": {"%": "", "count": ""}, "tags": {}}
    for rating in app_ratings:
        rating = rating.text
        rating = rating.replace("\t", "")
        rating = rating.replace("\n", "")
        rating = rating.replace("\r", "")
        rating = rating.replace("- ", "")
        if 'user reviews in the last 30 days' in rating:
            rating = rating.replace(" user reviews in the last 30 days are positive.", "")
            rating = rating.replace("of the ", "")
            rating = rating.split(' ')
            result['rating_30d'].update({"%": rating[0]})
            result['rating_30d'].update({"count": rating[1]})
        if 'user reviews for this game are positive' in rating:
            rating = rating.replace(" user reviews for this game are positive.", "")
            rating = rating.replace("of the ", "")
            rating = rating.replace(",", "")
            rating = rating.split(' ')
            result['rating_all_time'].update({"%": rating[0]})
            result['rating_all_time'].update({"count": rating[1]})
    app_tags = soup.find_all('a', class_='app_tag')
    for tags in app_tags:
        for tag in tags:
            tag = tag.replace("\n", "")
            tag = tag.replace("\r", "")
            while "	" in tag:  # This is not "space"!
                tag = tag.replace("	", "")
            result['tags'].update({tag: 'True'})
    app_content_makers = soup.find_all('div', class_='grid_content')
    for makers in app_content_makers:
        for maker in makers:
            maker = str(maker)
            while "	" in maker:
                maker = maker.replace("	", "")
            if 'developer' in maker:
                maker = maker.split('>')
                maker = maker[1]
                maker = maker.replace('</a', '')
                result.update({'developer': maker})
            if 'publisher' in maker:
                maker = maker.split('>')
                maker = maker[1]
                maker = maker.replace('</a', '')
                result.update({'publisher': maker})
    app_release_date = soup.find_all('div', class_='date')
    for date in app_release_date:
        date = str(date)
        date = date.replace('<div class="date">', '')
        date = date.replace('</div>', '')
        date = date.replace(',', '')
        date = datetime.strptime(date, '%d %b %Y')  # b - месяц словом
        date = str(date)
        date = date.split(' ')
        date = date[0]
        result.update({'date': date})

    return result

