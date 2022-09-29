from datetime import datetime
from os import path, makedirs, remove
from random import randint, uniform
from time import sleep
from ast import literal_eval
import logging

from pandas import DataFrame, concat, merge
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from requests import get, exceptions
from tqdm import tqdm

from .Logging_Config import logging_config
from .Universal_steam_statistics_luigi_task import my_beautiful_task_data_landing, \
    my_beautiful_task_data_frame_merge, my_beautiful_task_universal_parser_part

"""
Contains code for luigi task 'GetSteamProductsInfo'.
"""


def steam_apps_parser(interested_data: dict[DataFrame]) -> DataFrame:
    """
    Delete what is not a game at the stage of working with raw data.
    """
    is_not_application_list = ['Soundtrack', 'OST', 'Artbook', 'Texture', 'Demo', 'Playtest',
                               'test2', 'test3', 'Pieterw', 'Closed Beta', 'Open Beta', 'RPG Maker',
                               'Pack', 'Trailer', 'Teaser', 'Digital Art Book', 'Preorder Bonus']
    is_not_application_str, result_df = '|'.join(is_not_application_list), None
    for value in interested_data:
        interested_data = interested_data.get(value)
        interested_data = interested_data[~interested_data['name'].str.contains(
            is_not_application_str, regex=True)]
        null_filter = interested_data['name'] != ""
        result_df = my_beautiful_task_data_frame_merge(result_df, interested_data[null_filter])
    return result_df.reset_index(drop=True)


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
                result.update({'price': ['Free_to_Play']})
    if len(app_tag_dict.get('tags')) > 0:
        result.update({'tags': [str(app_tag_dict)]})
    else:
        if len(result) > 0:
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
                except Exception as connect_error:
                    try_number += 1
                    logging.error('Connect Retry... ' + str(try_number) + '\n' + str(connect_error) + '\n')
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
        result_dict.update({'scan_date': [date_today[0]]})
    return result_dict


def is_an_fake_user_must_be_registered(must_be_logged: BeautifulSoup.find_all) -> bool:
    """
    Check is fake user must be login to scrap steam product page.
    """
    if len(must_be_logged) > 0:
        for element in must_be_logged:
            if 'You must login to see this content' in str(element):
                return True
            else:
                return False
    else:
        return False


@connect_retry(10)
def ask_app_in_steam_store(app_id: str, app_name: str) -> list[dict, dict, bool]:
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

    must_be_logged = is_an_fake_user_must_be_registered(must_be_logged=soup.find_all('span', class_="error"))
    if must_be_logged is False:
        is_it_dls, interest_heads = soup.find_all('h1'), []
        for head in is_it_dls:
            interest_heads.append(str(head).replace('<h1>', '').replace('</h1>', ''))
        if 'Downloadable Content' not in interest_heads:  # Drope DLC
            scraping_steam_product(app_id, app_name, soup, result)
        else:  # Save DLC
            scraping_steam_product(app_id, app_name, soup, result_dlc)

    return [result, result_dlc, must_be_logged]


def safe_dict_data(path_to_file: str, date: str, df: DataFrame, file_name: str, ds_name: str):
    """
    Temporary storage, for landing raw data.
    """
    path_to_file = f"{path_to_file}/{date}/{ds_name}"
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
        with open(safe_dict_data_path, 'r') as safe_dict_data_file:
            rows = safe_dict_data_file.readlines()
        rows_len = len(rows) - 1
        if rows_len > 0:
            rows.pop(rows_len)
            with open(safe_dict_data_path, 'w') as safe_dict_data_file:
                for row in rows:
                    safe_dict_data_file.write(row)
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


def make_flag(partition_path: str):
    """
    Maike flags for empty collections.
    """
    output_path = f'{partition_path}'
    if not path.exists(output_path):
        makedirs(output_path)
    flag_path = f'{output_path}/{"_Validate_Success"}'
    with open(flag_path, 'w'):
        pass


def apps_and_dlc_df_landing(apps_df: DataFrame, apps_df_save_path: str,
                            dlc_df: DataFrame, dlc_df_save_path: str,
                            unsuitable_region_products_df: DataFrame, unsuitable_region_products_df_path: str,
                            products_not_for_unlogged_user_df: DataFrame, products_not_for_unlogged_user_df_path: str):
    """
    Lands real collections and maike flags if theme empty.
    """
    data_for_landing = {"Steam_Apps_Info.csv": [apps_df, apps_df_save_path],
                        "Steam_DLC_Info.csv": [dlc_df, dlc_df_save_path],
                        "Unsuitable_region_Products_Info.csv": [unsuitable_region_products_df,
                                                                unsuitable_region_products_df_path],
                        "Products_not_for_unlogged_user_Info.csv": [products_not_for_unlogged_user_df,
                                                                    products_not_for_unlogged_user_df_path]}
    for key in data_for_landing:
        if len(data_for_landing.get(key)[0]) != 0:
            my_beautiful_task_data_landing(data_for_landing.get(key)[0].replace(r'^\s*$', 'NULL', regex=True),
                                           data_for_landing.get(key)[1], key)
        else:
            make_flag(data_for_landing.get(key)[1])


def apps_and_dlc_list_validator(apps_df: DataFrame, apps_df_redy: DataFrame,
                                dlc_df: DataFrame, dlc_df_redy: DataFrame,
                                unsuitable_region_products_df: DataFrame,
                                unsuitable_region_products_df_redy: DataFrame,
                                products_not_for_unlogged_user_df: DataFrame,
                                products_not_for_unlogged_user_df_redy: DataFrame
                                ) -> list[DataFrame]:
    """
    Pandas DataFrame validator.
    Checks that the application and DLC collections are not empty.
    Merge real collections to land on.
    """
    data_for_validation = {"Steam_Apps_Info": [apps_df, apps_df_redy],
                           "Steam_DLC_Info": [dlc_df, dlc_df_redy],
                           "Unsuitable_region_Products_Info": [unsuitable_region_products_df,
                                                               unsuitable_region_products_df_redy],
                           "Products_not_for_unlogged_user_Info": [products_not_for_unlogged_user_df,
                                                                   products_not_for_unlogged_user_df_redy]}
    apps_and_dlc_df_list = []
    for key in data_for_validation:
        if type(data_for_validation.get(key)[0]) == type(None):
            apps_and_dlc_df_list.append([])
        else:
            apps_and_dlc_df_list.append(my_beautiful_task_data_frame_merge(data_for_validation.get(key)[0],
                                                                           data_for_validation.get(key)[1]))
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


def steam_product_scraping_validator(scraping_result: dict[str], get_steam_product_info_path: str, day_for_landing: str,
                                     product_data_frame: DataFrame, app_name: str, safe_name: str,
                                     catalogue_name: str
                                     ) -> DataFrame:
    """
    Validate scraping status of steam product then safe a result.
    """
    if scraping_result is not None and len(scraping_result) != 0:
        new_df_row: DataFrame = result_column_sort(scraping_result)
        safe_dict_data(get_steam_product_info_path, day_for_landing, new_df_row, safe_name, catalogue_name)
        product_data_frame: DataFrame = my_beautiful_task_data_frame_merge(product_data_frame, new_df_row)
        logging.info("'" + app_name + "' scraping succsessfully completed.")
    return product_data_frame


def unsuitable_products(app_id: str, app_name: str, get_steam_app_info_path: str,
                        day_for_landing: str, unsuitable_products_df: DataFrame,
                        safe_file_name: str, unsuitable_product_catalog: str,
                        logg_massage: str) -> DataFrame:
    """
    Unsuitable product safe, add to DataFrame and logging info massage.
    """
    date_today = str(datetime.today()).split(' ')
    new_df_row = DataFrame(data={'app_id': [app_id], 'app_name': [app_name], 'scan_date': [date_today[0]]})
    safe_dict_data(get_steam_app_info_path, day_for_landing, new_df_row,
                   safe_file_name, unsuitable_product_catalog)
    unsuitable_products_df: DataFrame = \
        my_beautiful_task_data_frame_merge(unsuitable_products_df, new_df_row)
    logging.info("'" + app_name + "' " + logg_massage)
    return unsuitable_products_df


def parsing_steam_data(interested_data: DataFrame, get_steam_app_info_path: str, day_for_landing: str,
                       apps_df: DataFrame or None, dlc_df: DataFrame or None,
                       unsuitable_region_products_df: DataFrame or None,
                       products_not_for_unlogged_user_df: DataFrame or None) -> list[DataFrame]:
    """
    Root function responsible for reading the local cache and
    merge it with parsed data from scraping steam application pages.
    Responsible for timeouts of get requests to application pages.
    """
    apps_safe_dict_data_path = f"{get_steam_app_info_path}/{day_for_landing}/{'Apps_info'}/{'_safe_dict_apps_data'}"
    dlc_safe_dict_data_path = f"{get_steam_app_info_path}/{day_for_landing}/{'DLC_info'}/{'_safe_dict_dlc_data'}"
    unsuitable_region_products_df_safe_dict_data_path = f"{get_steam_app_info_path}/{day_for_landing}" \
                                                        f"/{'Products_not_for_this_region_info'}/" \
                                                        f"{'_safe_dict_products_not_for_this_region_data'}"
    products_not_for_unlogged_user_df_safe_dict_data_path = f"{get_steam_app_info_path}/{day_for_landing}" \
                                                            f"/{'Products_not_for_unlogged_user_info'}/" \
                                                            f"{'_safe_dict_must_be_logged_to_scrapping_products'}"

    apps_df_redy = data_from_file_to_pd_dataframe(apps_safe_dict_data_path)
    dlc_df_redy = data_from_file_to_pd_dataframe(dlc_safe_dict_data_path)
    unsuitable_region_products_df_redy = \
        data_from_file_to_pd_dataframe(unsuitable_region_products_df_safe_dict_data_path)
    products_not_for_unlogged_user_df_redy = \
        data_from_file_to_pd_dataframe(products_not_for_unlogged_user_df_safe_dict_data_path)

    all_products_data_redy = concat([apps_df_redy['app_name'], dlc_df_redy['app_name'],
                                    unsuitable_region_products_df_redy['app_name'],
                                    products_not_for_unlogged_user_df_redy['app_name']],
                                    ).drop_duplicates().values
    common_all_products_data_redy = interested_data.merge(DataFrame({'name': all_products_data_redy}), on=['name'])
    interested_products = interested_data[~interested_data.name.isin(
        common_all_products_data_redy.name)].drop_duplicates().reset_index(drop=True)

    for index, tqdm_percent in zip(range(len(interested_products)),
                                   tqdm(range(len(interested_products) + len(common_all_products_data_redy)),
                                        desc="Scraping Steam products",
                                        unit=' SteamApp',
                                        ncols=120,
                                        # colour='green',
                                        initial=len(common_all_products_data_redy))):
        # time_wait = randint(1, 3)
        time_wait = uniform(0.1, 0.3)
        app_name = interested_products.iloc[index]['name']
        app_id = interested_products.iloc[index]['appid']

        if str(app_name) not in common_all_products_data_redy['name'].values:

            sleep(time_wait)
            result_list: list[dict, dict, bool] = ask_app_in_steam_store(app_id, app_name)
            result, result_dlc, must_be_logged = result_list[0], result_list[1], result_list[2]

            if must_be_logged is False:
                if int(len(result) + len(result_dlc)) > 0:
                    # App scraping result validate.
                    apps_df: DataFrame = steam_product_scraping_validator(result, get_steam_app_info_path,
                                                                          day_for_landing, apps_df, app_name,
                                                                          '_safe_dict_apps_data', 'Apps_info',)
                    # DLC scraping result validate.
                    dlc_df: DataFrame = steam_product_scraping_validator(result_dlc, get_steam_app_info_path,
                                                                         day_for_landing, dlc_df, app_name,
                                                                         '_safe_dict_dlc_data', 'DLC_info')
                else:  # Product not available in this region!  # BAG!
                    unsuitable_region_products_df: DataFrame = \
                        unsuitable_products(app_id, app_name, get_steam_app_info_path,
                                            day_for_landing, unsuitable_region_products_df,
                                            '_safe_dict_products_not_for_this_region_data',
                                            'Products_not_for_this_region_info',
                                            'product is not available in this region...')
            else:  # Fake user must be logged in steam for scraping this product page.
                products_not_for_unlogged_user_df: DataFrame = \
                    unsuitable_products(app_id, app_name, get_steam_app_info_path,
                                        day_for_landing, products_not_for_unlogged_user_df,
                                        '_safe_dict_must_be_logged_to_scrapping_products',
                                        'Products_not_for_unlogged_user_info',
                                        'product is not available for unlogged user...')
        else:
            logging.info("'" + app_name + "' already is in _safe_*_data...")
    apps_and_dlc_df_list: list[DataFrame] = apps_and_dlc_list_validator(apps_df, apps_df_redy, dlc_df, dlc_df_redy,
                                                                        unsuitable_region_products_df,
                                                                        unsuitable_region_products_df_redy,
                                                                        products_not_for_unlogged_user_df,
                                                                        products_not_for_unlogged_user_df_redy)
    return apps_and_dlc_df_list


def product_save_file_path(self, product: str, save_file: str) -> str:
    """
    Path generator.
    """
    return f"{self.get_steam_products_data_info_path}/{self.date_path_part:%Y/%m/%d}/{product}/{save_file}"


def get_product_save_file_path_list(self, products_save_file_list: list[str]) -> list[str]:
    """
    Path multy-generator.
    """
    result = []
    for dir_name in products_save_file_list:
        result.append(product_save_file_path(self, dir_name, ''))
    return result


def delete_temporary_safe_files(self, products_dict: dict[str]):
    """
    Delete temporary files for task.
    """
    for key in products_dict:
        file_path: str = product_save_file_path(self, key, products_dict.get(key))
        if path.isfile(file_path):
            remove(file_path)


def get_steam_products_data_info_steam_statistics_luigi_task_run(self):
    """
    Function for Luigi.Task.run()
    """
    logging_config(self.get_steam_products_data_info_logfile_path, int(self.get_steam_products_data_info_loglevel))
    result_successor = self.input()['AllSteamProductsData']
    interested_data: dict[DataFrame] = my_beautiful_task_universal_parser_part(result_successor, ".json")
    interested_data: DataFrame = steam_apps_parser(interested_data)
    apps_df, dlc_df, unsuitable_region_products_df, products_not_for_unlogged_user_df = None, None, None, None
    day_for_landing = f"{self.date_path_part:%Y/%m/%d}"

    apps_and_dlc_df_list: list[DataFrame] = parsing_steam_data(interested_data,
                                                               self.get_steam_products_data_info_path,
                                                               day_for_landing, apps_df, dlc_df,
                                                               unsuitable_region_products_df,
                                                               products_not_for_unlogged_user_df)
    apps_df, dlc_df, unsuitable_region_products_df, products_not_for_unlogged_user_df = \
        apps_and_dlc_df_list[0], apps_and_dlc_df_list[1], apps_and_dlc_df_list[2], apps_and_dlc_df_list[3]

    product_save_file_path_list: list[str] = \
        get_product_save_file_path_list(self, ['Apps_info', 'DLC_info', 'Products_not_for_this_region_info',
                                               'Products_not_for_unlogged_user_info'])
    apps_df_save_path, dlc_df_save_path, unsuitable_region_products_df_path, products_not_for_unlogged_user_df_path = \
        product_save_file_path_list[0], product_save_file_path_list[1], \
        product_save_file_path_list[2], product_save_file_path_list[3]

    apps_and_dlc_df_landing(apps_df, apps_df_save_path, dlc_df, dlc_df_save_path,
                            unsuitable_region_products_df, unsuitable_region_products_df_path,
                            products_not_for_unlogged_user_df, products_not_for_unlogged_user_df_path)

    delete_temporary_safe_files(self, {'Apps_info': '_safe_dict_apps_data',
                                       'DLC_info': '_safe_dict_dlc_data',
                                       'Products_not_for_this_region_info':
                                           '_safe_dict_products_not_for_this_region_data',
                                       "Products_not_for_unlogged_user_info":
                                           "_safe_dict_must_be_logged_to_scrapping_products"})

    make_flag(f"{self.get_steam_products_data_info_path}/{day_for_landing}")
