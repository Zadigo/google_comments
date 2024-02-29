import random
import string
import time

import pandas
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

from google_comments import (MEDIA_PATH, constants,
                             create_filename, get_selenium_browser_instance,
                             get_soup, logger, models, simple_clean_text,
                             text_parser)
from google_comments.base import SpiderMixin
from google_comments.models import GoogleBusiness
from google_comments.utilities import file_helpers
from google_comments.utilities.calculation import convert_coordinates
from google_comments.utilities.file_helpers import write_csv_file
from google_comments.utilities.text import slugify


class SearchLinks(SpiderMixin):
    """This automater reads a csv file called `search_data.csv`
    which contains a column called `data` containing a business
    name, an address and eventually a zip code. The concatenation
    of these three elements allows us to perform a search in the input
    of Google Maps in order to get the corresponding Google Place url"""

    URLS = []
    confusion_pages = []
    base_url = 'https://www.google.com/maps/@50.6476347,3.1369403,14z?entry=ttu'

    def __init__(self, output_folder=None, initial_data_file=None):
        self.driver = None
        self.data_file = None
        self.current_iteration = 0
        self.headless = False
        self.initial_data_file = initial_data_file or 'search_data.csv'
        self.output_filename = create_filename(prefix='search_urls')
        self.search_data_path = None
        super().__init__(output_folder=output_folder)

        screenshots_folder = MEDIA_PATH / 'screenshots'
        if not screenshots_folder.exists():
            screenshots_folder.mkdir()

    def create_file(self, prefix=None):
        df = pandas.DataFrame(data=self.URLS)
        df.to_csv(
            MEDIA_PATH / f'{self.output_filename}.csv',
            index=False,
            encoding='utf-8'
        )

    def before_launch(self):
        logger.info(f'Starting {self.__class__.__name__}...')

        self.search_data_path = search_data_path = MEDIA_PATH.joinpath(
            self.initial_data_file)
        df = pandas.read_csv(search_data_path, encoding='utf-8')
        if 'data' not in df.columns:
            raise ValueError("Your file should have a column 'data'")
        if not 'completed' in df.columns:
            df['completed'] = False
        else:
            df = df[df['completed'] == False]

        self.driver = get_selenium_browser_instance(headless=self.headless)
        self.driver.get(self.base_url)

        time.sleep(1)
        self.click_consent()
        self.driver.maximize_window()

        return df

    def start_spider(self, take_screenshots=False):
        df = self.before_launch()

        for item in df.itertuples(name='Search'):
            # Some data like "Althéa Fleurs, 1 Pl. du 8 Mai 1945, 16230 Mansle"
            # returns the google maps direction page which breaks the code
            # because in this case the search is a city. Continue on error
            # and return to the correct page.
            if '/maps/dir/' in self.current_page_url:
                logger.error(f"{item.data} triggers the wrong Google page")
                self.driver.get(self.base_url)
                continue

            input_script = """
            return document.querySelector('input[name="q"]')
            """
            element = self.driver.execute_script(input_script)
            element.clear()

            time.sleep(3)
            # After maximizing the Window, a small modal
            # about ads appears on the map. Run this
            # script to close it since it blocks our
            # actions on the page
            modal_script = """
            const el = document.querySelector('button[aria-label="Ignorer"]')
            el && el.click()
            """
            self.driver.execute_script(modal_script)

            actions = ActionChains(self.driver)
            actions.move_to_element(element)
            actions.click()
            actions.send_keys(item.data, Keys.ENTER)
            actions.perform()

            time.sleep(1)

            # Propose two options which is either
            # scroll the feed or indicate to be
            # an error page
            if self.is_feed_page:
                self.confusion_pages.append(
                    [item.data, self.driver.current_url])
                filename = f'failed_{self.output_filename}'
                write_csv_file(filename, self.confusion_pages)

                filename = create_filename(prefix=slugify(item.data))
                filepath = f'screenshots/{filename}.png'

                logger.warning(f'Is a feed page: "{item.data}"')

                if take_screenshots:
                    self.driver.get_screenshot_as_file(MEDIA_PATH / filepath)
                    logger.info(f'Created screenshot @ "{filepath}"')

                df.loc[item.Index, 'completed'] = True
                df.to_csv(self.search_data_path, index=False)

                self.current_iteration = self.current_iteration + 1
                time.sleep(random.randrange(15, 35))
                continue

            # When doing a click, a side modal opens
            # on the left of the screen
            modal_script = """
            const el = document.querySelector('button[jsaction="settings.close"]')
            el && el.click()
            """
            self.driver.execute_script(modal_script)

            # Allows the page to load on certain
            # search ites. Lag.
            time.sleep(5)
            current_page_url_script = """
            return window.location.href
            """
            url = self.driver.execute_script(current_page_url_script)
            if '/maps/place/' in url:
                data = {
                    'search': item.data,
                    'url': url,
                    'name': None,
                    'rating': None,
                    'number_of_reviews': None,
                    'latitude': None,
                    'longitude': None,
                    'coordinates': None,
                    'business_type': None,
                    'permanently_closed': False
                }

                try:
                    # While we are on the page, if we actually are on a /maps/place/
                    # implement simple additional pieces of the business eventually
                    # could be used later on
                    business_information = self.driver.execute_script(
                        constants.BUSINESS_INFORMATION_SCRIPT)
                except:
                    pass
                else:
                    model = GoogleBusiness(**business_information)
                    model.get_gps_coordinates_from_url()
                    data.update({
                        'name': model.name,
                        'rating': model.rating,
                        'number_of_reviews': None,
                        'latitude': model.latitude,
                        'longitude': model.longitude,
                        'coordinates': convert_coordinates(model.latitude, model.longitude),
                        'business_type': model.business_type,
                        'permanently_closed': model.permanently_closed
                    })

                self.URLS.append(data)
                self.current_page_actions()
                logger.info(f"Got url number {item.Index + 1}: \"{url}\"")
            else:
                self.URLS.append({
                    'search': item.data,
                    'url': None,
                    'name': None,
                    'rating': None,
                    'number_of_reviews': None,
                    'latitude': None,
                    'longitude': None,
                    'coordinates': None
                })
                logger.warning(f'Incorrect url for search: "{item.data}"')

            self.create_file()

            df.loc[item.Index, 'completed'] = True
            df.to_csv(self.search_data_path, index=False)

            time.sleep(random.randrange(30, 50))
            self.current_iteration = self.current_iteration + 1
            logger.info(
                f"Completed {self.current_iteration} "
                f"of {df['data'].count()}"
            )
        logger.info('Spider completed')


class SearchBusinesses(SearchLinks):
    """This automater reads a file containing searches to be executed
    for example `restaurants lille` in the search input of Google Maps
    and will then retrieve all the business information that are available 
    for the given query from the results page"""

    collected_search = []
    searches = []

    def flatten(self):
        return [search.as_json() for search in self.collected_search]

    def start_spider(self):
        df = self.before_launch()

        number_of_items = df['data'].count()
        for item in df.itertuples(name='Search'):
            input_script = """
            return document.querySelector('input[name="q"]')
            """
            element = self.driver.execute_script(input_script)
            element.clear()

            time.sleep(3)
            # After maximizing the Window, a small modal
            # about ads appears on the map. Run this
            # script to close it since it blocks our
            # actions on the page
            modal_script = """
            const el = document.querySelector('button[aria-label="Ignorer"]')
            el && el.click()
            """
            self.driver.execute_script(modal_script)

            actions = ActionChains(self.driver)
            actions.move_to_element(element)
            actions.click()
            actions.send_keys(item.data, Keys.ENTER)
            actions.perform()

            logger.info(f"Searched for: {item.data}")
            time.sleep(2)

            search = models.SearchedBusinesses(
                search_url=self.driver.current_url
            )

            self.current_iteration = self.current_iteration + 1
            # Propose two options which is either
            # scroll the feed or indicate to be
            # an error page
            if self.is_feed_page:
                self.scroll_feed()

                # TODO: Refactor code to make it
                # usable by multiple classes
                script = """
                let elements = document.querySelectorAll('div[role="feed"] div:not([class])')
                return elements
                """
                elements = self.driver.execute_script(script)

                number_of_business_cards = len(elements)
                logger.info(f'Found {number_of_business_cards} business cards')

                # 3. Iterate each business card
                iteration_count = 1
                while elements:
                    element = elements.pop()

                    content = element.get_attribute('innerHTML')
                    if content == '':
                        continue

                    iteration_count = iteration_count + 1

                    try:
                        link = element.find_element(By.TAG_NAME, 'a')
                    except:
                        logger.error(f"Could not find link within '{content}'")
                        time.sleep(1)
                        continue

                    try:
                        link.click()
                    except Exception as e:
                        logger.error(f'Could not click on business card')
                        logger.debug(e)
                        time.sleep(1)
                        continue
                    else:
                        time.sleep(1)

                    soup = get_soup(content)
                    link_tag = soup.find('a')
                    feed_url = link_tag.attrs.get('href')
                    business_name = link_tag.attrs.get('aria-label')

                    image = soup.find('span', attrs={'role': 'img'})
                    if image is not None:
                        rating_and_review_count = image.attrs.get('aria-label')
                        rating, review_count = rating_and_review_count.split(
                            ' ')

                        rating = text_parser(rating)
                        review_count = text_parser(review_count)
                    else:
                        rating = 0
                        review_count = 0

                    template = string.Template(constants.ADDRESS_SCRIPT)
                    more_information_script = template.substitute(
                        business_name=business_name
                    )
                    more_information = self.driver.execute_script(
                        more_information_script
                    )

                    time.sleep(5)

                    business = GoogleBusiness()
                    business.name = simple_clean_text(business_name)
                    business.rating = rating
                    business.number_of_reviews = review_count
                    business.url = self.current_page_url
                    business.feed_url = feed_url
                    business.address = more_information['address']
                    business.additional_information = simple_clean_text(
                        more_information['raw_information']
                    )
                    business.address = more_information['address']
                    business.telephone = more_information['telephone']
                    business.website = more_information['website']
                    business.get_gps_coordinates_from_feed_url()

                    search.places.append(business)
                    self.collected_businesses.append(business)
                    logger.info(
                        "Created business: "
                        f"'{business.name} @ {business.address}'"
                    )

                self.collected_search.append(search)
                self.searches.append(search)

                filename = create_filename(prefix='searched_business')
                file_helpers.write_json_file(self.output_folder_path.joinpath(
                    f'{filename}.json'),
                    self.flatten()
                )
                self.collected_search = []
                logger.info(
                    f'Collection completed for: "{item.data}": "{filename}"')
            else:
                self.confusion_pages.append(self.driver.current_url)
                logger.error(f'Failed to collect busineses for: "{item.data}"')
