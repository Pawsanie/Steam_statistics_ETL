from os import walk, path, makedirs, remove
import json
from pandas import DataFrame, read_csv, read_json
from numpy import NaN
from pyarrow import Table, parquet
from requests import get, exceptions
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from datetime import datetime
from random import randint
from time import sleep
from ast import literal_eval


def my_beautiful_task_data_landing(data_to_landing, day_for_landing, partition_path, file_mask):
    """Приземление распаршеных данных в виде json, csv, или parquet."""
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


def steam_apps_parser(interested_data):
    """Удаляем то что не является играми, на этапе работы с сырыми данными.."""
    for value in interested_data:
        all_aps_data = interested_data.get(value)
        all_aps_data = all_aps_data.iloc[0]
        all_aps_data = all_aps_data.to_dict()
        all_aps_data = all_aps_data.get('applist')
        interested_data = DataFrame(all_aps_data)
        interested_data = interested_data[~interested_data['name'].str.contains('Soundtrack')]
        interested_data = interested_data[~interested_data['name'].str.contains('OST')]
        interested_data = interested_data[~interested_data['name'].str.contains('Artbook')]
        interested_data = interested_data[~interested_data['name'].str.contains('Texture')]
        interested_data = interested_data[~interested_data['name'].str.contains('Demo')]
        interested_data = interested_data[~interested_data['name'].str.contains('Playtest')]
        interested_data = interested_data[~interested_data['name'].str.contains('DLC')]
        interested_data = interested_data[~interested_data['name'].str.contains('test2')]
        interested_data = interested_data[~interested_data['name'].str.contains('test3')]
        interested_data = interested_data[~interested_data['name'].str.contains('Pieterw')]
        interested_data = interested_data[~interested_data['name'].str.contains('Closed Beta')]
        interested_data = interested_data[~interested_data['name'].str.contains('Open Beta')]
        interested_data = interested_data[~interested_data['name'].str.contains('RPG Maker')]
        interested_data = interested_data[~interested_data['name'].str.contains('Pack')]
        null_filter = interested_data['name'] != ""
        interested_data = interested_data[null_filter]
        interested_data = interested_data.reset_index()
    return interested_data


def connect_retry(n):
    """
    Декоратор отвечающий за ретраи соединений,
    в случае ошибок.
    """
    def function_decor(function):
        def function_for_trying(*args, **kwargs):
            try_number = 0
            while try_number < n:
                try:
                    return function(*args, **kwargs)
                except:
                    try_number = try_number+1
                    print('Retry... ' + try_number)
        return function_for_trying
    return function_decor


@connect_retry(3)
def ask_app_in_steam_store(app_id, app_name):
    """Скрапинг страницы приложения."""
    print("\nTry to scraping: '" + app_name + "'")
    ua = UserAgent(cache=False, verify_ssl=False)
    scrap_user = {"User-Agent": str(ua.random), "Cache-Control": "no-cache", "Pragma": "no-cache"}
    app_page_url = f"{'https://store.steampowered.com/app'}/{app_id}/{app_name}"
    try:
        app_page = get(app_page_url, headers=scrap_user)
        soup = BeautifulSoup(app_page.text, "lxml")
        result = {}
        result.update({'app_id': [app_id]})
        result.update({'app_name': [app_name]})
        app_ratings = soup.find_all('span', class_='nonresponsive_hidden responsive_reviewdesc')
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
                result.update({"rating_30d_percent": [rating[0]]})
                result.update({"rating_30d_count": [rating[1]]})
            if 'user reviews for this game are positive' in rating:
                rating = rating.replace(" user reviews for this game are positive.", "")
                rating = rating.replace("of the ", "")
                rating = rating.replace(",", "")
                rating = rating.split(' ')
                result.update({"rating_all_time_percent": [rating[0]]})
                result.update({"rating_all_time_count": [rating[1]]})
        app_tags = soup.find_all('a', class_='app_tag')
        for tags in app_tags:
            for tag in tags:
                tag = tag.replace("\n", "")
                tag = tag.replace("\r", "")
                while "	" in tag:  # This is not "space"!
                    tag = tag.replace("	", "")
                result.update({tag: ['True']})
                if tag == 'Free to Play':
                    result.update({'price': ['0']})
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
                    result.update({'developer': [maker]})
                if 'publisher' in maker:
                    maker = maker.split('>')
                    maker = maker[1]
                    maker = maker.replace('</a', '')
                    result.update({'publisher': [maker]})
        app_release_date = soup.find_all('div', class_='date')
        for date in app_release_date:
            try:
                date = str(date)
                date = date.replace('<div class="date">', '')
                date = date.replace('</div>', '')
                date = date.replace(',', '')
                date = datetime.strptime(date, '%d %b %Y')  # b - месяц словом
                date = str(date)
                date = date.split(' ')
                date = date[0]
                result.update({'steam_release_date': date})
            except ValueError:
                result.update({'steam_release_date': 'in the pipeline'})
        date_today = str(datetime.today())
        date_today = date_today.split(' ')
        date_today = date_today[0]
        result.update({'scan_date': [date_today]})
        # if price have discount
        app_release_date = soup.find_all('div', class_='discount_block game_purchase_discount')
        for prices in app_release_date:
            for price in prices:
                price = str(price)
                if '%' not in price:
                    price = price.split('">')
                    price = price[2]
                    price = price.replace('</div><div class="discount_final_price', '')
                    result.update({'price': [price]})
        app_release_date = soup.find_all('div', class_='game_purchase_price price')
        for price in app_release_date:
            price = str(price)
            while "	" in price:  # This is not "space"!
                price = price.replace("	", "")
            price = price.split('div')
            price = price[1]
            price = price.replace("</", "")
            if 'data-price' in price:
                price = price.split('>')
                price = price[1]
                price = price.replace('\r\n', '')
                result.update({'price': [price]})
        return result
    except exceptions.SSLError as error:
        print(error)
        pass
    except exceptions.ConnectTimeout as error:
        print(error)
        pass


def safe_dict_data(path_to_file, date, df):
    """Временное хранилище, для загрузки сырых данных."""
    path_to_file = f"{path_to_file}/{date}"
    file_path = f"{path_to_file}/{'_safe_dict_data'}"
    df = str(df.to_dict()) + '\n'
    if not path.exists(path_to_file):
        makedirs(path_to_file)
    with open(file_path, 'a') as safe_file:
        safe_file.write(df)


def parsing_steam_data(interested_data, get_steam_app_info_path, day_for_landing, apps_df):
    safe_dict_data_path = f"{get_steam_app_info_path}/{day_for_landing}/{'_safe_dict_data'}"
    print(safe_dict_data_path)
    apps_df_redy = None
    if path.isfile(safe_dict_data_path):
        safe_dict_data_file = open(safe_dict_data_path, 'r')
        rows = safe_dict_data_file.readlines()
        rows_len = len(rows)-1
        if rows_len > 0:
            rows.pop(rows_len)
            safe_dict_data_file = open(safe_dict_data_path, 'w')
            for row in rows:
                safe_dict_data_file.write(row)
        else:
            remove(safe_dict_data_path)
        safe_dict_data_file.close()
        with open(safe_dict_data_path, 'r') as safe_dict_data_file:
            for row in safe_dict_data_file:
                row = DataFrame.from_dict(literal_eval(row.replace('\n', '')))
                apps_df_redy = my_beautiful_task_data_frame_merge(apps_df_redy, row)
    else:
        apps_df_redy = DataFrame({'app_name': []})
    for index in range(len(interested_data)):
        time_wait = randint(3, 6)
        app_name = interested_data.iloc[index]['name']
        app_id = interested_data.iloc[index]['appid']
        if str(app_name) not in apps_df_redy['app_name'].values:  # Have conflict with Numpy and Pandas.
            sleep(time_wait)
            result = ask_app_in_steam_store(app_id, app_name)
            new_df_row = DataFrame.from_dict(result)
            safe_dict_data(get_steam_app_info_path, day_for_landing, new_df_row)
            apps_df = my_beautiful_task_data_frame_merge(apps_df, new_df_row)
        else:
            print("'" + app_name + "' already is in _safe_dict_data...")
    apps_df = my_beautiful_task_data_frame_merge(apps_df_redy, apps_df)
    return apps_df


def get_csv_for_join(result_successor):
    """
    Создаёт корневой путь для csv.
    """
    path_to_table = str.replace(result_successor.path, '_Validate_Success', '')
    path_to_table = path_to_table.split('/')
    interested_data = []
    return interested_data
