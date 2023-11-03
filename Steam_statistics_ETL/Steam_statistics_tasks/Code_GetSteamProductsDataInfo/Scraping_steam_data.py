from datetime import datetime
import logging

from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from requests import get, Response
"""
Contains the cod for steam web page scraper.
"""


def connect_retry(maximum_iterations: int):
    """
    Decorator responsible for retrying connections in cases of errors.
    """
    def function_decor(function):
        def function_for_trying(*args, **kwargs):
            try_number: int = 0
            while try_number < maximum_iterations:
                try:
                    return function(*args, **kwargs)
                except Exception as connect_error:
                    try_number += 1
                    logging.error(f"Connect Retry... №{str(try_number)}\n{str(connect_error)}\n")
        return function_for_trying
    return function_decor


class SteamPageScraper:
    """
    Scraping app or DLC data from steam web page.
    """
    def __init__(self, *, app_id: str, app_name: str, soup: BeautifulSoup):
        """
        :param app_id: App id from steam ecosystem.
        :type app_id: str
        :param app_name: App or DLC name from steam.
        :type app_name: str
        :param soup: Steam shop web page BeautifulSoup object.
        :type soup: BeautifulSoup
        """
        self.app_id: str = app_id
        self.app_name: str = app_name
        self.soup: BeautifulSoup = soup
        self.result_dict: dict = {}

    def scraping_steam_product_rating(self, app_ratings: BeautifulSoup.find_all):
        """
        Scraping steam product rating.
        """
        for rating in app_ratings:
            rating: str = rating.text  # Not an error: BeautifulSoup.text -> str
            rating: str = rating.replace("\t", "").replace("\n", "").replace("\r", "").replace("- ", "")

            if 'user reviews in the last 30 days' in rating:
                rating: list[str] = rating.replace(" user reviews in the last 30 days are positive.", "") \
                    .replace("of the ", "") \
                    .split(' ')
                self.result_dict.update(
                    {
                        "rating_30d_percent": [rating[0]],
                        "rating_30d_count": [rating[1]]
                    }
                )

            if 'user reviews for this game are positive' in rating:
                rating: list[str] = rating.replace(" user reviews for this game are positive.", "") \
                    .replace("of the ", "") \
                    .replace(",", "") \
                    .split(' ')
                self.result_dict.update(
                    {
                        "rating_all_time_percent": [rating[0]],
                        "rating_all_time_count": [rating[1]]
                    }
                )

    def scraping_steam_product_tags(self, app_tags: BeautifulSoup.find_all):
        """
        Scraping steam product tags.
        """
        app_tag_dict: dict[str, list] = {'tags': []}
        for tags in app_tags:
            for tag in tags:
                tag = tag.replace("\n", "").replace("\r", "")
                while "	" in tag:  # This is not "space"!
                    tag: str = tag.replace("	", "")
                app_tag_dict.get('tags').append(tag)
                if tag == 'Free to Play':
                    self.result_dict.update({'price': ['Free_to_Play']})

        if len(app_tag_dict.get('tags')) > 0:
            self.result_dict.update({'tags': [str(app_tag_dict)]})
        else:
            if len(self.result_dict) > 0:
                self.result_dict.update({'tags': ''})

    def scraping_steam_product_maker(self, app_content_makers: BeautifulSoup.find_all):
        """
        Scraping steam product maker.
        """
        for makers in app_content_makers:
            for maker in makers:
                maker: str = str(maker)
                while "	" in maker:
                    maker: str = maker.replace("	", "")

                if 'developer' in maker:
                    maker: list[str] = maker.split('>')
                    maker: str = maker[1].replace('</a', '')
                    self.result_dict.update({'developer': [maker]})

                if 'publisher' in maker:
                    maker: list[str] = maker.split('>')
                    maker: str = maker[1].replace('</a', '')
                    self.result_dict.update({'publisher': [maker]})

    def scraping_steam_product_release_date(self, app_release_date: BeautifulSoup.find_all):
        """
        Scraping product steam release date.
        """
        for date in app_release_date:
            try:
                date: str = str(date)
                date: str = date.replace('<div class="date">', '') \
                    .replace('</div>', '') \
                    .replace(',', '')
                date: datetime = datetime.strptime(date, '%d %b %Y')  # b - month by a word
                date: list[str] = str(date).split(' ')
                date: str = date[0]
                self.result_dict.update({'steam_release_date': [date]})
            except ValueError:
                self.result_dict.update({'steam_release_date': ['in_the_pipeline']})

    def scraping_steam_product_price_with_discount(self, app_release_date: BeautifulSoup.find_all):
        """
        Scraping steam product steam price.
        For prices with discount.
        """
        for prices in app_release_date:
            for price in prices:
                price: str = str(price)
                if '%' not in price:
                    price: list[str] = price.split('">')
                    price: str = price[2].replace('</div><div class="discount_final_price', '')
                    self.result_dict.update({'price': [price]})

    def scraping_steam_product_price(self, app_release_date: BeautifulSoup.find_all):
        """
        Scraping steam product steam price.
        For normal prices.
        """
        for price in app_release_date:
            price: str = str(price)

            while "	" in price:  # This is not "space"!
                price: str = price.replace("	", "")
            price: list[str] = price.split('div')
            price: str = price[1].replace("</", "")

            if 'data-price' in price:
                price: list[str] = price.split('>')
                price: str = price[1].replace('\r\n', '')
                self.result_dict.update({'price': [price]})

    def scraping_is_available_in_steam(self, notice: BeautifulSoup.find_all):
        """
        Check that the application or DLC is available for purchase in Steam.
        """
        no_available_in_steam: str = 'is no longer available for sale on Steam.'
        available_notice: str = str(notice).replace('</div>', '')

        while "	" in available_notice:  # This is not "space"!
            available_notice: str = available_notice.replace("	", "")
        available_notice: list[str] = available_notice.split('\n')

        for data in available_notice:
            if no_available_in_steam in data:
                self.result_dict.update({'price': ['not_available_in_steam_now']})

    def scraping_steam_product(self) -> dict[str]:
        """
        Scraping conveyor.
        """
        # Steam product rating:
        app_ratings = self.soup.find_all('span', class_='nonresponsive_hidden responsive_reviewdesc')
        self.scraping_steam_product_rating(app_ratings)

        # Steam product maker:
        app_content_makers = self.soup.find_all('div', class_='grid_content')
        self.scraping_steam_product_maker(app_content_makers)

        # Product steam release date:
        app_release_date = self.soup.find_all('div', class_='date')
        self.scraping_steam_product_release_date(app_release_date)

        # If price have discount:
        app_release_date = self.soup.find_all('div', class_='discount_block game_purchase_discount')
        self.scraping_steam_product_price_with_discount(app_release_date)

        # If price have no discount:
        app_release_date = self.soup.find_all('div', class_='game_purchase_price price')
        self.scraping_steam_product_price(app_release_date)

        # Steam product tags:
        app_tags = self.soup.find_all('a', class_='app_tag')
        self.scraping_steam_product_tags(app_tags)

        # Is steam product available?:
        notice = self.soup.find_all('div', class_='notice_box_content')
        self.scraping_is_available_in_steam(notice)

        if len(self.result_dict) != 0:
            self.result_dict.update({'app_id': [self.app_id]})
            self.result_dict.update({'app_name': [self.app_name]})
            date_today = str(datetime.today()).split(' ')
            self.result_dict.update({'scan_date': [date_today[0]]})
        return self.result_dict


class ScrapingSteamData:
    """
    Application or DLC web page scraping.
    """
    @connect_retry(10)
    def ask_app_in_steam_store(self, app_id: str, app_name: str) -> tuple[dict[str], dict[str], bool]:
        """
        Application or DLC page scraping.
        """
        logging.info(f"Try to scraping: '{app_name}'")
        # fake_user = UserAgent(cache=False, verify_ssl=False).random  # UserAgent bag spam in logs. Wait new version.
        fake_user: str = UserAgent().random
        scrap_user: dict[str] = {"User-Agent": fake_user, "Cache-Control": "no-cache", "Pragma": "no-cache"}
        app_page_url: str = f"{'https://store.steampowered.com/app'}/{app_id}/{app_name}"
        app_page: Response = get(app_page_url, headers=scrap_user)
        soup: BeautifulSoup = BeautifulSoup(app_page.text, "lxml")
        result_apps, result_dlc = {}, {}

        must_be_logged: bool = self.is_an_fake_user_must_be_registered(soup.find_all('span', class_="error"))
        if must_be_logged is False:
            is_it_dls, interest_heads = soup.find_all('h1'), []
            for head in is_it_dls:
                interest_heads.append(str(head).replace('<h1>', '').replace('</h1>', ''))

            # Drope DLC:
            if 'Downloadable Content' not in interest_heads:
                result_apps: dict[str] = SteamPageScraper(
                    app_id=app_id,
                    app_name=app_name,
                    soup=soup) \
                    .scraping_steam_product()

            # Save DLC:
            else:
                result_dlc: dict[str] = SteamPageScraper(
                    app_id=app_id,
                    app_name=app_name,
                    soup=soup) \
                    .scraping_steam_product()

        return result_apps, result_dlc, must_be_logged

    @staticmethod
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
