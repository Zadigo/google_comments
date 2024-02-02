import datetime
import pytz
import pathlib
import time
from urllib.parse import quote_plus, urlencode, urlparse, quote

import pandas
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By

from google_comments import get_selenium_browser_instance, logger
from google_comments.base import SpiderMixin


class GoogleSearch(SpiderMixin):
    # collected_search = []

    def __init__(self, output_folder=None):
        self.driver = get_selenium_browser_instance()
        base_columns = [
            'search', 'url', 'gmaps_url',
            'address', 'telephone'
        ]
        self.collected_search = pandas.DataFrame([], columns=base_columns)
        super().__init__(output_folder=output_folder)

    def click_consent(self):
        """Function that clicks on the cookie 
        consent button"""
        script = """
        const buttonElement = document.querySelector('.KxvlWc')
        const xpath = '//div[contains(text(), "Tout accepter")][@role="none"]'
        const el = document.evaluate(xpath, buttonElement, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue
        el && el.click()
        """
        try:
            self.driver.execute_script(script)
        except:
            return False
        else:
            time.sleep(5)
            return True

    def before_launch(self):
        pass

    def iterate_urls(self, filename=None, searches=[]):
        if filename is not None:
            path = pathlib.Path(filename)
            with open(path, mode='r', encoding='utf-8') as f:
                df = pandas.read_csv(path)

                if 'terms' not in df.columns:
                    raise ValueError(
                        "Your file should contain a search column"
                    )
        else:
            df = pandas.DataFrame({'terms': searches})

        df['completed'] = False
        # for item in df.itertuples():
        #     self.start_spider(item.terms, is_loop=True)
        #     df.loc[item.Index, 'completed'] = True
        #     time.sleep(10)

        interval = datetime.timedelta(minutes=1)

        total_iterations = 0
        start_date = datetime.datetime.now(tz=pytz.UTC)
        next_execution_date = (start_date + interval)

        def is_valid(value):
            if value is None:
                return None

            instance = urlparse(value)
            if instance.scheme == '' or instance.netloc == '':
                return None

            return value

        while total_iterations < df.terms.count():
            current_date = datetime.datetime.now(tz=pytz.UTC)

            if total_iterations == 0 or current_date > next_execution_date:
                term = df.loc[total_iterations, 'terms']
                logger.info(f'Execution started for {term}')

                self.start_spider(quote(term), is_loop=True)
                df.loc[total_iterations, 'completed'] = True

                next_execution_date = next_execution_date + interval
                total_iterations = total_iterations + 1
                final_df = self.collected_search[[
                    'url', 'gmaps_url', 'address', 'telephone'
                ]]
                final_df['is_valid'] = final_df['url'].map(is_valid)
                final_df = final_df[~final_df['url'].isna()]
                final_df.to_csv('business.csv', index=False)
                df.to_csv('searches.csv', index=False)
            time.sleep(2)

    def start_spider(self, search, is_loop=False):
        self.before_launch()

        search = quote_plus(search)
        query = urlencode({'q': search})
        self.driver.get(f'https://www.google.com/search?{query}')
        self.click_consent()

        element = self.driver.find_element(By.TAG_NAME, 'body')
        html = element.get_attribute('innerHTML')

        contact_infos = self.driver.execute_script(
            """
            function evaluateXpath(xpath) {
                return document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue
            }

            function getText(el) {
                return el && el.textContent
            }

            const addressElement = evaluateXpath('//a[contains(@href, "/maps/place/")]')
            const telephone = getText(evaluateXpath('//span[contains(@aria-label, "Appeler le")]'))

            return [
                addressElement && addressElement.href,
                {
                    address: getText(addressElement),
                    telephone
                }
            ]
            """
        )

        soup = BeautifulSoup(html, 'html.parser')
        search_section = soup.find('div', {'id': 'search'})
        # [element.extract() for element in search_section.find_all('script')]
        search_section.script.decompose()
        # elements = search_section.find_all('div', {'class': 'TzHB6b'})
        elements = search_section.find_all('a')

        gmaps_url, business_data = contact_infos
        for element in elements:
            business_data = business_data | {
                'search': search, 'url': element.attrs.get('href')}
            self.collected_search = pandas.concat([
                self.collected_search,
                pandas.DataFrame([business_data])
            ])

        # for element in elements:
        #     h3_tag = element.find('h3')
        #     if h3_tag is not None:
        #         title = h3_tag.text
        #         business_data['title'] = title

        #         link_tag = element.find('a', {'jsname': 'UWckNb'})
        #         if link_tag is not None:
        #             link = link_tag.attrs.get('href')
        #             business_data['url'] = link

        #         self.collected_search = pandas.concat([
        #             self.collected_search,
        #             pandas.DataFrame([business_data])
        #         ])

        df = self.collected_search.sort_values('url').drop_duplicates()

        def remove_invalid_urls(url):
            if url is None:
                return False

            if url.startswith('/'):
                return False
            return True

        df['is_valid'] = df['url'].map(remove_invalid_urls)
        df = df[df['is_valid'] == True]

        if gmaps_url is not None:
            self.driver.get(gmaps_url)
            time.sleep(10)
            df['gmaps_url'] = self.driver.execute_script(
                """return window.location.href"""
            )

        if is_loop:
            pass
        else:
            df[['url', 'gmaps_url', 'address', 'telephone']].to_csv(
                'business.csv',
                index=False
            )
            self.driver.quit()


s = GoogleSearch()
# s.start_spider('Centre Commercial NICETOILE')
# s.start_spider('SAD Marketing')
# s.start_spider('intimissimi')
# s.start_spider('rouge gorge')
s.iterate_urls(filename='searches.csv')
