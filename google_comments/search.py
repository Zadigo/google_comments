import datetime
import pathlib
import time
from urllib.parse import quote_plus, urlencode, urlparse, urlunparse

import pandas
import pytz
from bs4 import BeautifulSoup
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

from google_comments import get_selenium_browser_instance, logger
from google_comments.base import SpiderMixin


class GoogleSearch(SpiderMixin):
    def __init__(self, output_folder=None):
        self.driver = get_selenium_browser_instance()
        self.is_loop = False
        super().__init__(output_folder=output_folder)

    def current_page_actions(self, search, urls, elements):
        pass

    def after_data_collection(self, df):
        """Actions to run on the page once the
        data was collected. This includes for example 
        updating the dataframe, saving the dataframe to
        a local file or any other data related actions"""
        pass

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

    def iterate_urls(self, *, use_input=False, filename=None, searches=[]):
        self.is_running = True
        self.is_loop = True

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

        interval = datetime.timedelta(minutes=1)

        total_iterations = 0
        start_date = datetime.datetime.now(tz=pytz.UTC)
        next_execution_date = (start_date + interval)

        while total_iterations < df.terms.count():
            current_date = datetime.datetime.now(tz=pytz.UTC)

            if total_iterations == 0 or current_date > next_execution_date:
                term = df.loc[total_iterations, 'terms']
                logger.info(f'Execution started for {term}')

                self.start_spider(term, use_input=use_input)
                df.loc[total_iterations, 'completed'] = True

                next_execution_date = next_execution_date + interval
                total_iterations = total_iterations + 1
                self.after_data_collection(df)
                # final_df = self.collected_search[[
                #     'url', 'gmaps_url', 'address', 'telephone'
                # ]]
                # final_df['is_valid'] = final_df['url'].map(is_valid)
                # final_df = final_df[~final_df['url'].isna()]
                # final_df.to_csv('business.csv', index=False)
                df.to_csv('searches.csv', index=False)
            time.sleep(2)

    def start_spider(self, search, use_input=False):
        """This function will start the spider but should be called
        directly. It should be calle within specific function that
        states the logic to parse the urls that were actually gathered
        on the Google Search page"""
        self.before_launch()

        url = None
        if use_input:
            url = f'https://www.google.com'
        else:
            search = quote_plus(search)
            query = urlencode({'q': search})
            url = f'https://www.google.com/search?{query}'

        self.driver.get(url)
        self.click_consent()

        if use_input:
            textarea = self.driver.execute_script(
                """return document.querySelector('textarea[name="q"]')"""
            )
            actions = ActionChains(self.driver)
            actions.move_to_element(textarea)
            actions.click()
            actions.send_keys_to_element(textarea, search, Keys.ENTER)
            actions.perform()
            time.sleep(5)

        element = self.driver.find_element(By.TAG_NAME, 'body')
        html = element.get_attribute('innerHTML')

        soup = BeautifulSoup(html, 'html.parser')
        search_section = soup.find('div', {'id': 'search'})
        # [element.extract() for element in search_section.find_all('script')]
        search_section.script.decompose()
        elements = search_section.find_all('a')

        urls = [element.attrs.get('href') for element in elements]
        self.current_page_actions(search, urls, elements)

        if not self.is_loop:
            self.driver.quit()


class BusinessSearch(GoogleSearch):
    def __init__(self, output_folder=None):
        base_columns = [
            'search', 'url', 'gmaps_url',
            'address', 'telephone'
        ]
        self.collected_search = pandas.DataFrame([], columns=base_columns)
        super().__init__(output_folder=output_folder)

    def after_data_collection(self, df):
        def is_valid(value):
            if value is None:
                return None

            instance = urlparse(value)
            if instance.scheme == '' or instance.netloc == '':
                return None

            return value

        final_df = self.collected_search[[
            'url', 'gmaps_url', 'address', 'telephone'
        ]]
        final_df['is_valid'] = final_df['url'].map(is_valid)
        final_df = final_df[~final_df['url'].isna()]
        final_df.to_csv('business.csv', index=False)

    def current_page_actions(self, search, urls, elements):
        """Returns information about a given business on
        the Google Search home page"""
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

        gmaps_url, business_data = contact_infos
        for element in elements:
            business_data = business_data | {
                'search': search, 'url': element.attrs.get('href')}
            self.collected_search = pandas.concat([
                self.collected_search,
                pandas.DataFrame([business_data])
            ])

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

        if self.is_loop:
            pass
        else:
            df[['url', 'gmaps_url', 'address', 'telephone']].to_csv(
                'business.csv',
                index=False
            )


class LinkedIn(GoogleSearch):
    def __init__(self, output_folder=None):
        base_columns = ['profiles', 'firstname', 'lastname']
        self.collected_search = pandas.DataFrame([], columns=base_columns)
        super().__init__(output_folder=output_folder)

    def current_page_actions(self, search, urls, elements):
        df = pandas.concat(
            [self.collected_search, pandas.DataFrame({'profiles': urls})]
        )

        def clean_url(url):
            """Remove the query part of 
            the current url"""
            if url is None:
                return None
            instance = urlparse(str(url))
            return urlunparse((
                instance.scheme,
                instance.netloc,
                instance.path,
                None,
                None,
                None
            ))

        def is_linkedin_profile(url):
            if url is None:
                return False

            if str(url).startswith('/'):
                return False

            if 'linkedin.com/in/' in str(url):
                return True

            return False

        df['profiles'] = df['profiles'].map(clean_url)
        df['is_linkedin'] = df['profiles'].map(is_linkedin_profile)

        profiles_df = df.query('is_linkedin == True')
        profiles_df = profiles_df.sort_values(
            'profiles').drop_duplicates()

        for item in profiles_df.itertuples(name='Profile'):
            instance = urlparse(item.profiles)
            result = instance.path.split('/')

            user_information = result[-1].split('-')
            try:
                if len(user_information) == 1:
                    # ex./in/cecilejolly
                    firstname = user_information[0]
                    lastname = None
                elif len(user_information) == 2:
                    # ex./in/clervie-fournier
                    firstname, lastname = user_information
                elif len(user_information) == 3:
                    # ex. /in/filipa-teixeira-6397b6213
                    firstname, lastname = user_information[:-1]
            except:
                firstname = ' '.join(user_information)
                lastname = None

            if firstname is not None:
                profiles_df.loc[item.Index, 'firstname'] = firstname.title()

            if lastname is not None:
                profiles_df.loc[item.Index, 'lastname'] = lastname.title()

        profiles_df['date'] = datetime.datetime.now(tz=pytz.UTC)
        profiles_df.to_csv('profiles.csv', index=False)


# s = LinkedIn()
# s.get_business_profile('Centre Commercial NICETOILE')
# s.get_business_profile('SAD Marketing')
# s.get_business_profile('intimissimi')
# s.get_business_profile('rouge gorge')
# s.iterate_urls(filename='searches.csv')
# s.start_spider(
#     'site:linkedin.com/in kedge',
#     use_input=True
# )

s = BusinessSearch()
s.iterate_urls(filename='searches.csv', use_input=True)
