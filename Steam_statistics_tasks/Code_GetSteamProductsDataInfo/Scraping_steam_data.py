from datetime import datetime
import logging

from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from requests import get


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


class ScrapingSteamData:
    def is_an_fake_user_must_be_registered(self, must_be_logged: BeautifulSoup.find_all) -> bool:
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

    def scraping_steam_product_rating(self, app_ratings: BeautifulSoup.find_all, result: dict) -> dict[str]:
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

    def scraping_steam_product_tags(self, app_tags: BeautifulSoup.find_all, result: dict) -> dict[str]:
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

    def scraping_steam_product_maker(self, app_content_makers: BeautifulSoup.find_all, result: dict) -> dict[str]:
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

    def scraping_steam_product_release_date(self, app_release_date: BeautifulSoup.find_all, result: dict) -> dict[str]:
        """
        Scraping product steam release date.
        """
        for date in app_release_date:
            try:
                date = str(date)
                date = date.replace('<div class="date">', '') \
                    .replace('</div>', '') \
                    .replace(',', '')
                date = datetime.strptime(date, '%d %b %Y')  # b - month by a word
                date = str(date).split(' ')
                date = date[0]
                result.update({'steam_release_date': [date]})
            except ValueError:
                result.update({'steam_release_date': ['in_the_pipeline']})
        return result

    def scraping_steam_product_price_with_discount(self, app_release_date: BeautifulSoup.find_all, result: dict) -> dict[str]:
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

    def scraping_steam_product_price(self, app_release_date: BeautifulSoup.find_all, result: dict) -> dict[str]:
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

    def scraping_is_available_in_steam(self, notice: BeautifulSoup.find_all, result: dict) -> dict[str]:
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

    def scraping_steam_product(self, app_id: str, app_name: str, soup: 'BeautifulSoup["lxml"]',
                               result_dict: dict) -> dict[str]:
        """
        Scraping conveyor.
        """
        # Steam product rating.
        app_ratings = soup.find_all('span', class_='nonresponsive_hidden responsive_reviewdesc')
        self.scraping_steam_product_rating(app_ratings, result_dict)
        # Steam product maker.
        app_content_makers = soup.find_all('div', class_='grid_content')
        self.scraping_steam_product_maker(app_content_makers, result_dict)
        # Product steam release date.
        app_release_date = soup.find_all('div', class_='date')
        self.scraping_steam_product_release_date(app_release_date, result_dict)
        # If price have discount.
        app_release_date = soup.find_all('div', class_='discount_block game_purchase_discount')
        self.scraping_steam_product_price_with_discount(app_release_date, result_dict)
        # If price have no discount.
        app_release_date = soup.find_all('div', class_='game_purchase_price price')
        self.scraping_steam_product_price(app_release_date, result_dict)
        # Steam product tags.
        app_tags = soup.find_all('a', class_='app_tag')
        self.scraping_steam_product_tags(app_tags, result_dict)
        # Is steam product available?
        notice = soup.find_all('div', class_='notice_box_content')
        self.scraping_is_available_in_steam(notice, result_dict)
        if len(result_dict) != 0:
            result_dict.update({'app_id': [app_id]})
            result_dict.update({'app_name': [app_name]})
            date_today = str(datetime.today()).split(' ')
            result_dict.update({'scan_date': [date_today[0]]})
        return result_dict

    @connect_retry(10)
    def ask_app_in_steam_store(self, app_id: str, app_name: str) -> list[dict, dict, bool]:
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

        must_be_logged = self.is_an_fake_user_must_be_registered(must_be_logged=soup.find_all('span', class_="error"))
        if must_be_logged is False:
            is_it_dls, interest_heads = soup.find_all('h1'), []
            for head in is_it_dls:
                interest_heads.append(str(head).replace('<h1>', '').replace('</h1>', ''))
            if 'Downloadable Content' not in interest_heads:  # Drope DLC
                self.scraping_steam_product(app_id, app_name, soup, result)
            else:  # Save DLC
                self.scraping_steam_product(app_id, app_name, soup, result_dlc)

        return [result, result_dlc, must_be_logged]