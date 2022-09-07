from datetime import datetime
from os import walk, path, makedirs, remove
from random import randint
from time import sleep
from ast import literal_eval
import logging

from pandas import DataFrame
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from requests import get, exceptions

from .Universal_steam_statistics_luigi_task import my_beautiful_task_data_landing, \
    my_beautiful_task_data_frame_merge, my_beautiful_task_universal_parser_part
"""
Contains code for luigi task 'GetSteamAppInfo'.
"""


def steam_apps_parser(interested_data: dict[DataFrame]) -> DataFrame:
    """
    Delete what is not a game at the stage of working with raw data.
    '''
    Удаляет то что не является играми, на этапе работы с сырыми данными.
    """
    for value in interested_data:
        interested_data = interested_data.get(value)
        interested_data = interested_data[~interested_data['name'].str.contains('Soundtrack')]
        interested_data = interested_data[~interested_data['name'].str.contains('OST')]
        interested_data = interested_data[~interested_data['name'].str.contains('Artbook')]
        interested_data = interested_data[~interested_data['name'].str.contains('Texture')]
        interested_data = interested_data[~interested_data['name'].str.contains('Demo')]
        interested_data = interested_data[~interested_data['name'].str.contains('Playtest')]
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


def scraping_steam_product_rating(app_ratings: BeautifulSoup.find_all, result: dict) -> dict[str]:
    """
    Scraping steam product rating.
    """
    for rating in app_ratings:
        rating = rating.text
        rating = rating.replace("\t", "").replace("\n", "").replace("\r", "").replace("- ", "")

        if 'user reviews in the last 30 days' in rating:
            rating = rating.replace(" user reviews in the last 30 days are positive.", "") \
                            .replace("of the ", "") \
                            .split(' ')
            result.update({"rating_30d_percent": [rating[0]],
                           "rating_30d_count": [rating[1]]})

        if 'user reviews for this game are positive' in rating:
            rating = rating.replace(" user reviews for this game are positive.", "") \
                            .replace("of the ", "") \
                            .replace(",", "") \
                            .split(' ')
            result.update({"rating_all_time_percent": [rating[0]],
                           "rating_all_time_count": [rating[1]]})
    return result


def scraping_steam_product_tags(app_tags: BeautifulSoup.find_all, result: dict) -> dict[str]:
    """
    Scraping steam product tags.
    """
    app_tag_dict = {'tags': []}
    for tags in app_tags:
        for tag in tags:
            tag = tag.replace("\n", "").replace("\r", "")
            while "	" in tag:  # This is not "space"!
                tag = tag.replace("	", "")
            app_tag_dict.get('tags').append(tag)
            if tag == 'Free to Play':
                result.update({'price': ['0']})
    if len(app_tag_dict.get('tags')) > 0:
        result.update({'tags': [str(app_tag_dict)]})
    else:
        result.update({'tags': ''})
    return result


def scraping_steam_product_maker(app_content_makers: BeautifulSoup.find_all, result: dict) -> dict[str]:
    """
    Scraping steam product maker.
    """
    for makers in app_content_makers:
        for maker in makers:
            maker = str(maker)
            while "	" in maker:
                maker = maker.replace("	", "")
            if 'developer' in maker:
                maker = maker.split('>')
                maker = maker[1].replace('</a', '')
                result.update({'developer': [maker]})
            if 'publisher' in maker:
                maker = maker.split('>')
                maker = maker[1].replace('</a', '')
                result.update({'publisher': [maker]})
    return result


def scraping_steam_product_release_date(app_release_date: BeautifulSoup.find_all, result: dict) -> dict[str]:
    """
    Scraping product steam release date.
    """
    for date in app_release_date:
        try:
            date = str(date)
            date = date.replace('<div class="date">', '')\
                       .replace('</div>', '')\
                       .replace(',', '')
            date = datetime.strptime(date, '%d %b %Y')  # b - month by a word
            date = str(date).split(' ')
            date = date[0]
            result.update({'steam_release_date': [date]})
        except ValueError:
            result.update({'steam_release_date': ['in_the_pipeline']})
    return result


def scraping_steam_product_price_with_discount(app_release_date: BeautifulSoup.find_all, result: dict) -> dict[str]:
    """
    Scraping steam product steam price.
    For prices with discount.
    """
    for prices in app_release_date:
        for price in prices:
            price = str(price)
            if '%' not in price:
                price = price.split('">')
                price = price[2].replace('</div><div class="discount_final_price', '')
                result.update({'price': [price]})
    return result


def scraping_steam_product_price(app_release_date: BeautifulSoup.find_all, result: dict) -> dict[str]:
    """
    Scraping steam product steam price.
    For normal prices.
    """
    for price in app_release_date:
        price = str(price)
        while "	" in price:  # This is not "space"!
            price = price.replace("	", "")
        price = price.split('div')
        price = price[1].replace("</", "")
        if 'data-price' in price:
            price = price.split('>')
            price = price[1].replace('\r\n', '')
            result.update({'price': [price]})
    return result


def scraping_is_available_in_steam(notice: BeautifulSoup.find_all, result: dict) -> dict[str]:
    """
    Check that the application or DLC is available for purchase in Steam.
    """
    no_available_in_steam = 'is no longer available for sale on Steam.'
    available_notice = str(notice).replace('</div>', '')
    while "	" in available_notice:  # This is not "space"!
        available_notice = available_notice.replace("	", "")
    available_notice = available_notice.split('\n')
    for data in available_notice:
        if no_available_in_steam in data:
            result.update({'price': ['not_available_in_steam_now']})
    return result


def connect_retry(maximum_iterations: int):
    """
    Decorator responsible for retrying connections in cases of errors.
    """
    def function_decor(function):
        def function_for_trying(*args, **kwargs):
            try_number = 0
            while try_number < maximum_iterations:
                try:
                    return function(*args, **kwargs)
                except exceptions as connect_error:
                    try_number += 1
                    logging.error('Connect Retry... ' + str(try_number) + '\n' + connect_error)
        return function_for_trying
    return function_decor


def scraping_steam_product(app_id: str, app_name: str, soup: 'BeautifulSoup["lxml"]', result_dict: dict) -> dict[str]:
    """
    Scraping conveyor.
    """
    # Steam product rating.
    app_ratings = soup.find_all('span', class_='nonresponsive_hidden responsive_reviewdesc')
    scraping_steam_product_rating(app_ratings, result_dict)
    # Steam product maker.
    app_content_makers = soup.find_all('div', class_='grid_content')
    scraping_steam_product_maker(app_content_makers, result_dict)
    # Product steam release date.
    app_release_date = soup.find_all('div', class_='date')
    scraping_steam_product_release_date(app_release_date, result_dict)
    # If price have discount.
    app_release_date = soup.find_all('div', class_='discount_block game_purchase_discount')
    scraping_steam_product_price_with_discount(app_release_date, result_dict)
    # If price have no discount.
    app_release_date = soup.find_all('div', class_='game_purchase_price price')
    scraping_steam_product_price(app_release_date, result_dict)
    # Steam product tags.
    app_tags = soup.find_all('a', class_='app_tag')
    scraping_steam_product_tags(app_tags, result_dict)
    # Is steam product available?
    notice = soup.find_all('div', class_='notice_box_content')
    scraping_is_available_in_steam(notice, result_dict)
    if len(result_dict) != 0:
        result_dict.update({'app_id': [app_id]})
        result_dict.update({'app_name': [app_name]})
        date_today = str(datetime.today()).split(' ')
        date_today = date_today[0]
        result_dict.update({'scan_date': [date_today]})
    return result_dict


@connect_retry(3)
def ask_app_in_steam_store(app_id: str, app_name: str) -> list[dict, dict]:
    """
    Application page scraping.
    """
    logging.info("Try to scraping: '" + app_name + "'")
    # fake_user = UserAgent(cache=False, verify_ssl=False).random  # UserAgent bag spam in logs. Wait new version.
    fake_user = UserAgent().random
    scrap_user = {"User-Agent": fake_user, "Cache-Control": "no-cache", "Pragma": "no-cache"}
    app_page_url = f"{'https://store.steampowered.com/app'}/{app_id}/{app_name}"
    app_page = get(app_page_url, headers=scrap_user)
    soup = BeautifulSoup(app_page.text, "lxml")
    result, result_dlc = {}, {}

    is_it_dls, interest_heads = soup.find_all('h1'), []
    for head in is_it_dls:
        interest_heads.append(str(head).replace('<h1>', '').replace('</h1>', ''))
    if 'Downloadable Content' not in interest_heads:  # Drope DLC
        scraping_steam_product(app_id, app_name, soup, result)
    else:  # Save DLC
        scraping_steam_product(app_id, app_name, soup, result_dlc)

    result_list = [result, result_dlc]
    return result_list


def safe_dict_data(path_to_file: str, date: str, df: DataFrame, file_name: str, ds_name: str):
    """
    Temporary storage, for landing raw data.
    '''
    Временное хранилище, для загрузки сырых данных.
    """
    path_to_file = f"{path_to_file}/{ds_name}/{date}"
    file_path = f"{path_to_file}/{file_name}"
    df = df.to_dict('records')
    df = str(df[0]) + '\n'
    if not path.exists(path_to_file):
        makedirs(path_to_file)
    with open(file_path, 'a') as safe_file:
        safe_file.write(df)


def data_from_file_to_pd_dataframe(safe_dict_data_path: str) -> DataFrame:
    """
    Reads the local cache.
    """
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
            logging.info('Start merge local_cash...')
            with open(safe_dict_data_path, 'r') as safe_dict_data_file:
                data = safe_dict_data_file.read()
                apps_df_redy = DataFrame.from_dict(literal_eval(data.replace('\n', ',')))
                apps_df_redy.reset_index(drop=True)
                logging.info(str(apps_df_redy) + '\nLocal_cash successfully merged...')
        else:
            remove(safe_dict_data_path)
            apps_df_redy = DataFrame({'app_name': []})
    else:
        apps_df_redy = DataFrame({'app_name': []})
    return apps_df_redy


def make_flag(partition_path: str, day_for_landing: str):
    """
    Maike flags for empty collections.
    """
    output_path = f'{partition_path}/{day_for_landing}'
    if not path.exists(output_path):
        makedirs(output_path)
    flag_path = f'{output_path}/{"_Validate_Success"}'
    flag = open(flag_path, 'w')
    flag.close()


def apps_and_dlc_df_landing(apps_df: DataFrame, dlc_df: DataFrame, day_for_landing: str,
                            apps_df_save_path: str, dlc_df_save_path: str):
    """
    Lands real collections and maike flags if theme empty.
    """
    if len(apps_df) != 0:
        my_beautiful_task_data_landing(apps_df, day_for_landing, apps_df_save_path, "Get_Steam_App_Info.csv")
    else:
        make_flag(apps_df_save_path, day_for_landing)
    if len(dlc_df) != 0:
        my_beautiful_task_data_landing(dlc_df, day_for_landing, dlc_df_save_path, "Get_Steam_DLC_Info.csv")
    else:
        make_flag(dlc_df_save_path, day_for_landing)


def apps_and_dlc_list_validator(apps_df: DataFrame, apps_df_redy: DataFrame,
                                dlc_df: DataFrame, dlc_df_redy: DataFrame) -> list[DataFrame]:
    """
    Pandas DataFrame validator.
    Checks that the application and DLC collections are not empty.
    Merge real collections to land on.
    """
    if type(apps_df) == type(None):
        apps_df = []
    else:
        apps_df = my_beautiful_task_data_frame_merge(apps_df_redy, apps_df)
    if type(dlc_df) == type(None):
        dlc_df = []
    else:
        dlc_df = my_beautiful_task_data_frame_merge(dlc_df_redy, dlc_df)
    apps_and_dlc_df_list = [apps_df, dlc_df]
    return apps_and_dlc_df_list


def result_column_sort(interest_dict: dict) -> DataFrame:
    """
    Sort columns and mayke DF from apps or DLC dict.
    """
    new_df_row = DataFrame.from_dict(interest_dict)
    inserted_columns = ['app_id', 'app_name', 'developer', 'rating_all_time_percent',
                        'rating_all_time_count', 'rating_30d_percent', 'rating_30d_count',
                        'publisher', 'price', 'steam_release_date', 'tags', 'scan_date']
    row_data_frame_head = new_df_row.head()
    for column_name in inserted_columns:
        if column_name not in row_data_frame_head:
            new_df_row[column_name] = ''
    new_df_row = new_df_row[inserted_columns]
    return new_df_row


def steam_product_scraping_validator(scraping_result: dict[str], get_steam_app_info_path: str, day_for_landing: str,
                                     product_data_frame: DataFrame, app_id: str, app_name: str, safe_name: str,
                                     catalogue_name: str, product_not_for_region_catalog: str,
                                     not_available_log_masege: str):
    """
    Validate scraping status of steam product then safe a result.
    """
    # Steam product scraping succsessfully.
    if scraping_result is not None and len(scraping_result) != 0:
        new_df_row = result_column_sort(scraping_result)
        safe_dict_data(get_steam_app_info_path, day_for_landing, new_df_row, safe_name, catalogue_name)
        product_data_frame = my_beautiful_task_data_frame_merge(product_data_frame, new_df_row)
        logging.info("'" + app_name + "' scraping succsessfully completed.")
    # Steam product scrapping failed.
    else:
        new_df_row = DataFrame(data={'app_id': [app_id], 'app_name': [app_name]})
        safe_dict_data(get_steam_app_info_path, day_for_landing, new_df_row,
                       safe_name, product_not_for_region_catalog)
        logging.info("'" + app_name + "' " + not_available_log_masege)
    return product_data_frame


def parsing_steam_data(interested_data: DataFrame, get_steam_app_info_path: str, day_for_landing: str,
                       apps_df: DataFrame or None, dlc_df: DataFrame or None) -> list[DataFrame]:
    """
    Root function responsible for reading the local cache and
    merge it with parsed data from scraping steam application pages.
    Responsible for timeouts of get requests to application pages.
    '''
    Корневая функция, отвечающая за чтение локального кэша,
    его мёрдж с распаршеными данными от скрапинга страниц приложений steam.
    Отвечает за таймауты при get запросах к страницам приложений.
    """
    safe_dict_data_path = f"{get_steam_app_info_path}/{'Apps_info'}/{day_for_landing}/{'_safe_dict_data'}"
    dlc_dict_data_path = f"{get_steam_app_info_path}/{'DLC_info'}/{day_for_landing}/{'_safe_dict_dlc_data'}"
    apps_df_redy = data_from_file_to_pd_dataframe(safe_dict_data_path)
    dlc_df_redy = data_from_file_to_pd_dataframe(dlc_dict_data_path)
    for index in range(len(interested_data)):  # Get app data to data frame
        time_wait = randint(1, 3)
        app_name = interested_data.iloc[index]['name']
        app_id = interested_data.iloc[index]['appid']
        # 1 rows below have conflict with Numpy and Pandas. Might cause errors in the future.
        if str(app_name) not in apps_df_redy['app_name'].values and str(app_name) not in dlc_df_redy['app_name'].values:
            sleep(time_wait)
            result_list = ask_app_in_steam_store(app_id, app_name)
            result, result_dlc = result_list[0], result_list[1]
            # App scraping result validate.
            if len(result) != 0:
                apps_df = steam_product_scraping_validator(result, get_steam_app_info_path,
                                                           day_for_landing, apps_df, app_id, app_name,
                                                           '_safe_dict_data', 'Apps_info',
                                                           'Apps_not_for_this_region',
                                                           'app is not available in this region...')
            # DLC scraping result validate.
            if len(result_dlc) != 0:
                dlc_df = steam_product_scraping_validator(result_dlc, get_steam_app_info_path,
                                                          day_for_landing, dlc_df, app_id, app_name,
                                                          '_safe_dict_dlc_data', 'DLC_info',
                                                          'DLC_not_for_this_region',
                                                          'DLC is not available in this region...')
        else:
            logging.info("'" + app_name + "' already is in _safe_*_data...")
    apps_and_dlc_df_list = apps_and_dlc_list_validator(apps_df, apps_df_redy, dlc_df, dlc_df_redy)
    return apps_and_dlc_df_list


def safe_dlc_data(get_steam_app_info_path: str) -> DataFrame or None:
    """
    Collects data from the DLC, then parses it into a pandas DataFrame.
    """
    root_path = f"{get_steam_app_info_path}/{'DLC_info'}"
    dlc_df = None
    interested_data = None
    file_list = []
    if path.exists(root_path):
        for dirs, folders, files in walk(root_path):
            for file in files:
                path_to_file = f'{dirs}/{file}'
                file_list.append(path_to_file)
        if len(file_list) != 0:
            interested_data = my_beautiful_task_universal_parser_part(file_list, '.csv', drop_list=None)
    if interested_data is not None:
        for data in interested_data.values():
            dlc_df = my_beautiful_task_data_frame_merge(dlc_df, data)
    else:
        dlc_df = None
    return dlc_df
