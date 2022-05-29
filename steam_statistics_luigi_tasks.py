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


def my_beautiful_task_data_frame_merge(data_from_files, extract_data):
    """Объеденяет переданные датафреймы в один, заполняя  NaN пустые ячейки."""
    if data_from_files is None:
        data_from_files = extract_data
    else:
        new_point_for_merge = extract_data.columns.difference(data_from_files.columns)
        for column in new_point_for_merge:
            data_from_files.astype(object)[column] = NaN
        data_from_files = data_from_files.merge(extract_data, how='outer')
        data_from_files = data_from_files.reset_index(drop=True)
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


def steam_aps_from_web_api_parser(interested_data):
    """Парсит результат получаемый от Steam Web-API."""
    all_aps_data = interested_data
    all_aps_data = all_aps_data.get('applist')
    all_aps_data = all_aps_data.get('apps')
    return all_aps_data


def steam_apps_parser(interested_data):
    """Удаляем то что не является играми, на этапе работы с сырыми данными.."""
    for value in interested_data:
        interested_data = interested_data.get(value)
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
        interested_data = interested_data[~interested_data['name'].str.contains('Trailer')]
        interested_data = interested_data[~interested_data['name'].str.contains('Teaser')]
        interested_data = interested_data[~interested_data['name'].str.contains('Digital Art Book')]
        null_filter = interested_data['name'] != ""
        interested_data = interested_data[null_filter]
        interested_data = interested_data.reset_index(drop=True)
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
                    try_number = try_number + 1
                    print('Retry... ' + str(try_number))

        return function_for_trying

    return function_decor


@connect_retry(3)
def ask_app_in_steam_store(app_id, app_name):
    """Скрапинг страницы приложения."""
    print("\nTry to scraping: '" + app_name + "'")
    ua = UserAgent(cache=False, verify_ssl=False)
    scrap_user = {"User-Agent": str(ua.random), "Cache-Control": "no-cache", "Pragma": "no-cache"}
    app_page_url = f"{'https://store.steampowered.com/app'}/{app_id}/{app_name}"
    app_page = get(app_page_url, headers=scrap_user)
    soup = BeautifulSoup(app_page.text, "lxml")
    result = {}
    result_dlc = {}
    is_it_dls = soup.find_all('h1')
    for head in is_it_dls:  # Drope DLC
        head = str(head)
        if 'Downloadable Content' not in head:
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
            if len(result) != 0:
                result.update({'app_id': [app_id]})
                result.update({'app_name': [app_name]})
                date_today = str(datetime.today())
                date_today = date_today.split(' ')
                date_today = date_today[0]
                result.update({'scan_date': [date_today]})
        else:  # Save DLC
            result_dlc.update({'app_id': [app_id]})
            result_dlc.update({'app_name': [app_name]})
    result_list = [result, result_dlc]
    return result_list


def safe_dict_data(path_to_file, date, df, file_name, ds_name):
    """Временное хранилище, для загрузки сырых данных."""
    path_to_file = f"{path_to_file}/{ds_name}/{date}"
    file_path = f"{path_to_file}/{file_name}"
    df = df.to_dict('records')
    df = str(df[0]) + '\n'
    if not path.exists(path_to_file):
        makedirs(path_to_file)
    with open(file_path, 'a') as safe_file:
        safe_file.write(df)


def data_from_file_to_pd_dataframe(safe_dict_data_path):
    """Читает локальный кэш."""
    apps_df_redy = None
    if path.isfile(safe_dict_data_path):
        safe_dict_data_file = open(safe_dict_data_path, 'r')
        rows = safe_dict_data_file.readlines()
        rows_len = len(rows) - 1
        if rows_len > 0:
            rows.pop(rows_len)
            safe_dict_data_file = open(safe_dict_data_path, 'w')
            for row in rows:
                safe_dict_data_file.write(row)
            safe_dict_data_file.close()
            print('Start merge local_cash...')
            with open(safe_dict_data_path, 'r') as safe_dict_data_file:
                data = safe_dict_data_file.read()
                apps_df_redy = DataFrame.from_dict(literal_eval(data.replace('\n', ',')))
                apps_df_redy = apps_df_redy.reset_index(drop=True)
                print(apps_df_redy)
                print('Local_cash successfully merged...')
        else:
            remove(safe_dict_data_path)
            apps_df_redy = DataFrame({'app_name': []})
    else:
        apps_df_redy = DataFrame({'app_name': []})
    return apps_df_redy


def make_flag(partition_path, day_for_landing):
    """Проставляет флаги для пустых колекций."""
    output_path = f'{partition_path}/{day_for_landing}'
    if not path.exists(output_path):
        makedirs(output_path)
    flag_path = f'{output_path}/{"_Validate_Success"}'
    flag = open(flag_path, 'w')
    flag.close()


def apps_and_dlc_df_landing(apps_df, dlc_df, day_for_landing, apps_df_save_path, dlc_df_save_path):
    """
    Приземляет реально существующие коллекции и проставляет флаги, для пустых.
    """
    if len(apps_df) != 0:
        my_beautiful_task_data_landing(apps_df, day_for_landing, apps_df_save_path, "Get_Steam_App_Info.csv")
    else:
        make_flag(apps_df_save_path, day_for_landing)
    if len(dlc_df) != 0:
        my_beautiful_task_data_landing(dlc_df, day_for_landing, dlc_df_save_path, "Get_Steam_DLC_Info.csv")
    else:
        make_flag(dlc_df_save_path, day_for_landing)


def apps_and_dlc_list_validator(apps_df, apps_df_redy, dlc_df, dlc_df_redy):
    """
    Валидатор pandas DataFrame.
    Проверяет что коллекции приложений и DLC не пустые.
    Мёрджит реально существующие коллекции, для приземления.
    """
    if type(apps_df) == type(None):
        # apps_df.empty
        apps_df = []
    else:
        apps_df = my_beautiful_task_data_frame_merge(apps_df_redy, apps_df)
    if type(dlc_df) == type(None):
        # dlc_df.empty:
        dlc_df = []
    else:
        print(type(dlc_df))
        dlc_df = my_beautiful_task_data_frame_merge(dlc_df_redy, dlc_df)
    apps_and_dlc_df_list = [apps_df, dlc_df]
    return apps_and_dlc_df_list


def parsing_steam_data(interested_data, get_steam_app_info_path, day_for_landing, apps_df, dlc_df):
    """
    Корневая переменная, отвечающая за чтение локального кэша,
    его мёрдж с распаршеными данными от скрапинга страниц приложений steam.
    Отвечает за таймауты при get запросах к страницам приложений.
    """
    safe_dict_data_path = f"{get_steam_app_info_path}/{'Apps_info'}/{day_for_landing}/{'_safe_dict_data'}"
    dlc_dict_data_path = f"{get_steam_app_info_path}/{'DLC_info'}/{day_for_landing}/{'_safe_dict_dlc_data'}"
    apps_df_redy = data_from_file_to_pd_dataframe(safe_dict_data_path)
    dlc_df_redy = data_from_file_to_pd_dataframe(dlc_dict_data_path)
    for index in range(len(interested_data)):  # Get app data to data frame
        time_wait = randint(3, 6)
        app_name = interested_data.iloc[index]['name']
        app_id = interested_data.iloc[index]['appid']
        # 2 rows below have conflict with Numpy and Pandas. Might cause errors in the future.
        if str(app_name) not in apps_df_redy['app_name'].values:
            if str(app_name) not in dlc_df_redy['app_name'].values:
                sleep(time_wait)
                result_list = ask_app_in_steam_store(app_id, app_name)
                result = result_list[0]
                result_dlc = result_list[1]
                if result is not None and len(result) != 0:
                    new_df_row = DataFrame.from_dict(result)
                    inserted_columns = ['app_id', 'app_name']
                    new_columns = ([col for col in inserted_columns if col in new_df_row]
                                   + [col for col in new_df_row if col not in inserted_columns])
                    new_df_row = new_df_row[new_columns]
                    safe_dict_data(get_steam_app_info_path, day_for_landing, new_df_row, '_safe_dict_data', 'Apps_info')
                    apps_df = my_beautiful_task_data_frame_merge(apps_df, new_df_row)
                if result_dlc is not None and len(result_dlc) != 0:
                    new_df_row = DataFrame.from_dict(result_dlc)
                    safe_dict_data(get_steam_app_info_path, day_for_landing, new_df_row,
                                   '_safe_dict_dlc_data', 'DLC_info')
                    dlc_df = my_beautiful_task_data_frame_merge(dlc_df, new_df_row)
            else:
                print("'" + app_name + "' is dlc and has not been processed...")
        else:
            print("'" + app_name + "' already is in _safe_dict_data...")
    apps_and_dlc_df_list = apps_and_dlc_list_validator(apps_df, apps_df_redy, dlc_df, dlc_df_redy)
    return apps_and_dlc_df_list


def get_csv_for_join(result_successor):
    """
    Создаёт корневой путь для csv.
    Затем парсит его, с целью получить все csv таблицы для объединения.
    """
    root_path = result_successor.path
    symbol_counts = len(root_path)
    root_path = root_path[:symbol_counts - 28]
    file_list = []
    for dirs, folders, files in walk(root_path):
        for file in files:
            path_to_file = f'{dirs}/{file}'
            file_list.append(path_to_file)
    interested_data = my_beautiful_task_universal_parser_part(file_list, '.csv', drop_list=None)
    return interested_data


def steam_apps_data_cleaning(all_apps_data_frame):
    """
    Очищает all_apps_data_frame от приложений, которые не являются играми.
    """
    # 'app_which_not_game' требует дополнения, по результатам тестирования ->
    app_which_not_game = ['Animation & Modeling', 'Game Development', 'Tutorial']
    all_apps_data_frame_heads = all_apps_data_frame.head()
    for index in range(len(all_apps_data_frame)):
        for column_name in all_apps_data_frame_heads:
            column_name = str(column_name)
            column_name = all_apps_data_frame.iloc[index][column_name]
            if str(column_name) in app_which_not_game:
                all_apps_data_frame = all_apps_data_frame.drop(all_apps_data_frame.index[index], inplace=True)
    all_apps_data_frame = all_apps_data_frame.reset_index(drop=True)
    return all_apps_data_frame


def safe_dlc_data(get_steam_app_info_path):
    """Собирает данные с DLC, затем парсит их в pandas DataFrame."""
    root_path = f"{get_steam_app_info_path}/{'DLC_info'}"
    dlc_df = None
    file_list = []
    if path.exists(root_path):
        for dirs, folders, files in walk(root_path):
            for file in files:
                path_to_file = f'{dirs}/{file}'
                file_list.append(path_to_file)
        if len(file_list) != 0:
            dlc_df = my_beautiful_task_universal_parser_part(file_list, '.csv', drop_list=None)
    return dlc_df
