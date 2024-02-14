import argparse
import csv
import json
import pathlib
import random
import re
import secrets
import string
import sys
import time
from collections import defaultdict

import pandas
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

from google_comments import (MEDIA_PATH, check_url, clean_dict, constants,
                             create_filename, get_selenium_browser_instance,
                             get_soup, logger, models, simple_clean_text,
                             text_parser)
from google_comments.base import SpiderMixin
from google_comments.models import GoogleBusiness, Review
from google_comments.utilities import file_helpers
from google_comments.utilities.calculation import convert_coordinates
from google_comments.utilities.file_helpers import write_csv_file
from google_comments.utilities.text import slugify

COMMENTS_SCROLL_ATTEMPTS = 500

# COMMENTS_UPDATE_SCROLL_ATTEMPTS = 2

FEED_SCROLL_ATTEMPTS = 30

COMMENTS_SCROLL_WAIT_TIME = 10


class GoogleMapsMixin(SpiderMixin):
    COMMENTS = []
    collect_reviews = True
    keep_unique_file = False

    def __init__(self, output_folder=None, headless=False):
        # Identifies the unique ID for the current
        # scroll session which then can be retraced
        # by the elements that need to
        self.scrap_session_id = f'gc_{secrets.token_hex(10)}'
        self.driver = get_selenium_browser_instance(headless=headless)
        self.websocket = None
        self.seen_urls_outputted = False
        self.filename = None
        self.is_loop = False
        super().__init__(output_folder=output_folder)
        logger.info('Starting spider')

    def __repr__(self):
        return f'<{self.__class__.__name__} [{self.scrap_session_id}]>'

    def __del__(self):
        try:
            self.driver.quit()
            sys.exit(0)
        except:
            logger.info('Program stopped')

    def __hash__(self):
        return hash((self.scrap_session_id))

    def sort_comments(self):
        open_menu = """
        try {
            function sortComments() {
                let sortButton = (
                    document.querySelector('button[aria-label*="Sort reviews"][data-value^="Sort"]') ||
                    document.querySelector('button[aria-label*="Trier les avis"][data-value^="Trier"]')
                )
                sortButton && sortButton.click()
            }
            sortComments()
        } catch (e) {
            console.error(e)
        }
        """

        click_radio = """
        try {
            function clickRadio () {
                let menu = document.querySelector('div[id="action-menu"][role="menu"]')
                let menuOption = menu && menu.querySelectorAll('div[role="menuitemradio"]')

                let newestRadio = menuOption[1]
                newestRadio && newestRadio.click()
            }
            clickRadio()
        } catch (e) {
            console.error(e)
        }
        """
        try:
            self.driver.execute_script(open_menu)
            time.sleep(2)
            self.driver.execute_script(click_radio)
        except:
            logger.error('Could not sort comments')
        else:
            logger.info('Comments sorted')
            time.sleep(3)

    def flatten(self):
        """Flatten the saved dataclasses to dictionnaries"""
        return [business.as_json() for business in self.collected_businesses]

    # TODO: Simplify this section
    def create_comments_dataframe(self, *, save=True, columns=['text', 'rating']):
        """Return the comments using only a specific set
        of columns and eventually save the file"""
        df = pandas.DataFrame(self.COMMENTS, columns=columns)

        def remove_punctuation(text):
            if text is None:
                return None
            return text.replace(';', ' ').replace(',', ' ')
        df['text'] = df['text'].apply(remove_punctuation)
        df = df.sort_values('text')

        if save:
            filename = self.filename or create_filename(
                suffix='clean_comments'
            )
            df.to_csv(
                self.output_folder_path.joinpath(f'{filename}.csv'),
                index=False,
                encoding='utf-8'
            )
        return df

    # TODO: Simplify this section pandas
    def create_files(self, business, filename):
        """Create the files to store the comments, the business information
        and the clean comments as a csv. This will also trigger the
        registered webhooks"""
        with open(self.output_folder_path.joinpath(f'business_{filename}.json'), mode='w') as fp1:
            json.dump(self.flatten(), fp1)

        if self.collect_reviews:
            with open(self.output_folder_path.joinpath(f'{filename}_comments.json'), mode='w') as fp2:
                json.dump(self.COMMENTS, fp2)
            self.create_comments_dataframe()
            logger.info(f'Created files: {filename} and {filename}_comments')

        # TODO: Reimplement this functionnality later on
        # self.trigger_webhooks(data=business.as_json())
        logger.info(f'Created files: business_{filename}')


class GooglePlaces(GoogleMapsMixin):
    """This automater uses a Google Maps link that references
    a feed of multiple places to scrap data from : /maps/search/"""

    def get_dataframe(self):
        data = defaultdict(list)
        for business in self.flatten():
            for review in business['reviews']:
                data['google_review_id'].append(review.google_review_id)
                data['text'].append(review['text'])
                data['rating'].append(review['rating'])
                data['period'].append(review.period)
                data['reviewer_name'].append(review['reviewer_name'])
                data['reviewer_number_of_reviews'].append(
                    review['reviewer_number_of_reviews']
                )

                data['date'].append(business['date'])
                data['name'].append(business['name'])
                data['url'].append(business['url'])
                data['feed_url'].append(business['feed_url'])
                data['address'].append(business['address'])
                data['company_rating'].append(business['rating'])
                data['latitude'].append(business['longitude'])
                data['number_of_reviews'].append(business['number_of_reviews'])
        return pandas.DataFrame(data)

    def start_spider(self, url):
        self.is_running = True

        self.filename = filename = create_filename()
        urls_seen_file = pathlib.Path(MEDIA_PATH / f'{filename}_urls_seen.csv')
        if not urls_seen_file.exists():
            urls_seen_file.touch()
            logger.info(f'Created file: {filename}')

        self.driver.maximize_window()
        self.driver.get(url)

        # 1. Click on the consent form
        self.click_consent()

        # 2. Scroll the feed
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
                continue

            try:
                link.click()
            except:
                logger.error(f'Could not click on business card')
                continue
            else:
                time.sleep(1)
                self.sort_comments()

            soup = get_soup(content)
            link_tag = soup.find('a')
            feed_url = link_tag.attrs.get('href')
            business_name = link_tag.attrs.get('aria-label')

            image = soup.find('span', attrs={'role': 'img'})
            if image is not None:
                rating_and_review_count = image.attrs.get('aria-label')
                rating, review_count = rating_and_review_count.split(' ')

                rating = text_parser(rating)
                review_count = text_parser(review_count)
            else:
                rating = 0
                review_count = 0

            address_script = """
            function getText (el) {
                return el && el.textContent
            }

            function resolveXpath (xpath) {
                return document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue
            }

            function evaluateXpath (xpath) {
                var result = resolveXpath(xpath)
                return getText(result)
            }

            return {
                feed_url: window.location.href,
                address: evaluateXpath('//button[contains(@data-item-id, "address")]'),
                telephone: evaluateXpath('//button[contains(@data-item-id, "tel")]'),
                website: evaluateXpath('//a[contains(@data-item-id, "authority")]'),
                raw_information: evaluateXpath('//div[contains(@aria-label, "Informations")][@role="region"][contains(@class, "m6QErb")]')
            }
            """
            more_information_script = string.Template(
                address_script).substitute(business_name=business_name)
            more_information = self.driver.execute_script(
                more_information_script
            )

            time.sleep(5)

            business = GoogleBusiness()
            business.name = simple_clean_text(business_name)
            business.rating = rating
            business.number_of_reviews = review_count
            business.url = url
            business.feed_url = feed_url
            business.address = more_information['address']
            business.additional_information = simple_clean_text(
                more_information['raw_information']
            )
            business.address = more_information['address']
            business.telephone = more_information['telephone']
            business.website = more_information['website']
            business.get_gps_coordinates_from_url(substitute_url=url)

            seen_urls_path = self.output_folder_path.joinpath(
                f'{filename}_urls_seen.csv'
            )

            with open(seen_urls_path, newline='\n', mode='a') as f:
                writer = csv.writer(f)
                writer.writerow([business.name, business.feed_url])

            logger.info(
                f'Created business: "{business.name} @ {business.address}"'
            )

            if self.collect_reviews:
                # Click on the comments tab
                tab_list = self.driver.find_elements(
                    By.CSS_SELECTOR,
                    '*[role="tablist"] button'
                )
                try:
                    tab_list[1].click()
                except:
                    continue
                time.sleep(2)

                # TODO: Check if this potentially
                # breaks the code when business_name
                # is None
                # if business_name is None:
                #     logger.info(f"Business name not found for url: {url}")
                #     continue

                if "'" in business_name:
                    business_name = business_name.replace("'", "\\'")

                if '"' in business_name:
                    business_name = business_name.replace('"', '\\"')

                # Iteration for each review

                count = 0
                pixels = 2000
                last_positions = []
                return_position = 0
                while count < COMMENTS_SCROLL_ATTEMPTS:
                    scroll_bottom_script = """
                    const mainWrapper = document.querySelector('div[role="main"][aria-label="$business_name"]')
                    const el = mainWrapper.querySelector('div[tabindex="-1"]')
                    el.scroll({ top: $pixels, left: 0, behavior: "instant" })
                    return [ el.scrollTop, el.scrollHeight ]
                    """
                    scroll_bottom_script = string.Template(scroll_bottom_script).substitute(
                        business_name=business_name,
                        pixels=pixels
                    )

                    try:
                        current_scroll, scroll_height = self.driver.execute_script(
                            scroll_bottom_script
                        )
                    except:
                        logger.error('Could not scroll to bottom on comments')

                    if current_scroll > 0:
                        # When the current_scroll is in the last
                        # three positions, we can safely break
                        # the looop otherwise we'll have to
                        # to the max of COMMENTS_SCROLL_ATTEMPTS
                        if current_scroll in last_positions[:3]:
                            last_positions = []
                            break
                    last_positions.append(current_scroll)

                    # Increase the number of pixels to
                    # get when we reach a certain level
                    # of scrolling on the page
                    if current_scroll > 10000:
                        return_position = random.choice(last_positions[5:])

                    if current_scroll > 10000:
                        pixels = pixels + 8000
                    elif current_scroll > 20000:
                        pixels = pixels + 15000
                    else:
                        pixels = pixels + 2000

                    scroll_top_script = """
                    const mainWrapper = document.querySelector('div[role="main"][aria-label="$business_name"]')
                    const el = mainWrapper.querySelector('div[tabindex="-1"]')
                    el.scroll({ top: $return_position, left: 0, behavior: "smooth" })
                    """
                    scroll_top_script = string.Template(scroll_top_script).substitute(
                        business_name=business_name,
                        return_position=return_position
                    )
                    self.driver.execute_script(scroll_top_script)

                    count = count + 1
                    # logger.debug(f'Completed {count} of {COMMENTS_SCROLL_ATTEMPTS} scrolls')
                    time.sleep(5)

                comments_script = """
                function getText (el) {
                    return el && el.textContent.trim()
                }

                function resolveXpath (xpath) {
                    return document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue
                }

                function evaluateXpath (xpath) {
                    var result = resolveXpath(xpath)
                    return getText(result)
                }

                function gatherComments() {
                    const commentsWrapper = document.querySelectorAll("div[data-review-id^='Ch'][class*='fontBodyMedium ']")

                    Array.from(commentsWrapper).forEach((item) => {
                        let dataReviewId = item.dataset['reviewId']
                        try {
                            // Sometimes there is a read more button
                            // that we have to click

                            moreButton = (
                                // Try the "Voir plus" button"
                                item.querySelector('button[aria-label="Voir plus"]') ||
                                // Try the "See more" button"
                                item.querySelector('button[aria-label="See more"]') ||
                                // On last resort try "aria-expanded"
                                item.querySelector('button[aria-expanded="false"]')
                            )
                            moreButton.click()
                        } catch (e) {
                            console.log('No "see more" button for review', dataReviewId)
                        }
                    })

                    return Array.from(commentsWrapper).map((item) => {
                        let dataReviewId = item.dataset['reviewId']

                        // Or, .rsqaWe
                        let period = getText(item.querySelector('.DU9Pgb'))
                        let rating = item.querySelector('span[role="img"]') && item.querySelector('span[role="img"]').ariaLabel
                        let text = getText(item.querySelector("*[class='MyEned']"))
                        let reviewerName = getText(item.querySelector('[class*="d4r55"]'))
                        let reviewerNumberOfReviews = getText(item.querySelector('*[class*="RfnDt"]'))

                        return {
                            google_review_id: dataReviewId,
                            text,
                            rating,
                            period,
                            reviewer_name: reviewerName,
                            reviewer_number_of_reviews: reviewerNumberOfReviews
                        }
                    })
                }

                return gatherComments()
                """
                comments = self.driver.execute_script(comments_script)
                logger.info(f'Collected {len(comments)} comments')

                for comment in comments:
                    clean_comment = clean_dict(comment)

                    text = clean_comment['text']
                    if text is not None:
                        text1 = text.replace(';', ' ')
                        text2 = text1.replace(',', ' ')
                        clean_comment['text'] = text2

                    instance = Review(**clean_comment)
                    business.reviews.append(instance)
                    self.COMMENTS.append(clean_comment)

            self.collected_businesses.append(business)
            self.create_files(business, filename)

            logger.info(
                f"Completed {iteration_count} of "
                f"{number_of_business_cards} business cards"
            )

        if not is_loop:
            self.is_running = False
            self.driver.quit()


class GooglePlace(GoogleMapsMixin):
    """Gets information about a Google Place business and
    eventually the reviews that were left by the users. A Google Place
    url is required for this automater to function `/maps/place/`"""

    def start_spider(self, url, comments_scroll_attempts=None, is_loop=False, maximize_window=True):
        if not self.is_running:
            self.is_running = True

        if maximize_window:
            self.driver.maximize_window()

        # if not self.keep_unique_file and self.filename is not None:
        self.filename = filename = create_filename(suffix=id_or_reference)
        self.driver.get(url)

        # 1. Click on the consent form - This appears
        # when we first try to use the Google Maps
        # website in anonymous state
        self.click_consent()

        # 2. Get the business information: name, website, address etc.
        details = self.driver.execute_script(
            constants.BUSINESS_INFORMATION_SCRIPT
        )

        number_of_reviews = details['number_of_reviews']
        if number_of_reviews is not None:
            result = re.search(r'(\d+)', number_of_reviews)

            if result:
                details['number_of_reviews'] = result.group(1)

        details = clean_dict(details)
        business = GoogleBusiness(**details)

        # Get the business url once again because the coordinates
        # can get slightly updated once the map loads completly
        updated_business_url = self.driver.execute_script(
            """return window.location.href"""
        )
        business.url = updated_business_url

        business.get_gps_coordinates_from_url()

        # 3. Get all/if not most of the reviews left
        # for the current business. NOTE: When trying
        # to click on the "See more" button of a review,
        # we don't necessarily get all reviewer's text. We
        # could find a technique to correct that
        if self.collect_reviews:
            tab_list = self.driver.find_elements(
                By.CSS_SELECTOR,
                '*[role="tablist"] button'
            )
            try:
                tab_list[1].click()
            except:
                return False

            time.sleep(2)

            self.sort_comments()

            business_name = details['name']
            try:
                # When we could not get a business
                # name, just safely return otherwsise
                # we get a TypeError when trying to
                # to check quotes in the value with
                # the conditional IF
                if "'" in business_name:
                    business_name = business_name.replace("'", "\\'")

                if '"' in business_name:
                    business_name = business_name.replace('"', '\\"')
            except TypeError as e:
                logger.error(f"Value for business name is None: {e}")
                return False

            count = 0
            pixels = 2000
            last_positions = []
            return_position = 0
            comments_scroll_attempts = comments_scroll_attempts or COMMENTS_SCROLL_ATTEMPTS
            while count < comments_scroll_attempts:
                scroll_bottom_script = """
                const mainWrapper = document.querySelector('div[role="main"][aria-label="$business_name"]')
                const el = mainWrapper.querySelector('div[tabindex="-1"]')
                el.scroll({ top: $pixels, left: 0, behavior: "instant" })
                return [ el.scrollTop, el.scrollHeight ]
                """
                scroll_bottom_script = string.Template(scroll_bottom_script).substitute(
                    business_name=business_name,
                    pixels=pixels
                )

                try:
                    current_scroll, scroll_height = self.driver.execute_script(
                        scroll_bottom_script
                    )
                except Exception as e:
                    logger.error(
                        "Could not scroll the comments container "
                        "because the related section could not be found"
                    )
                    logger.critical(e)
                    return False

                self.comments_scroll_counter.update({current_scroll: 1})
                result = self.test_current_scroll_repetition(
                    self.comments_scroll_counter,
                    current_scroll
                )
                if result:
                    # DEBUG: Check the counter
                    # logger.debug(f'{dict(self.comments_scroll_counter)}')
                    self.comments_scroll_counter.clear()
                    break

                last_positions.append(current_scroll)

                # Increase the number of pixels to
                # get when we reach a certain level
                # of scrolling on the page
                if current_scroll > 10000:
                    return_position = random.choice(last_positions[5:])

                if current_scroll > 10000:
                    pixels = pixels + 8000
                elif current_scroll > 20000:
                    pixels = pixels + 15000
                else:
                    pixels = pixels + 2000

                scroll_top_script = """
                const mainWrapper = document.querySelector('div[role="main"][aria-label="$business_name"]')
                const el = mainWrapper.querySelector('div[tabindex="-1"]')
                el.scroll({ top: $return_position, left: 0, behavior: "smooth" })
                """
                scroll_top_script = string.Template(scroll_top_script).substitute(
                    business_name=business_name,
                    return_position=return_position
                )
                self.driver.execute_script(scroll_top_script)

                count = count + 1
                logger.debug(
                    f"Completed {count} of "
                    f"{COMMENTS_SCROLL_ATTEMPTS} scrolls"
                )
                time.sleep(COMMENTS_SCROLL_WAIT_TIME)

            # Execute the script two times so that we can get
            # comments with a lot of text
            # self.driver.execute_script(constants.COMMENTS_SCRIPT)
            comments = self.driver.execute_script(constants.COMMENTS_SCRIPT)
            logger.info(f'Collected {len(comments)} comments')

            for comment in comments:
                clean_comment = clean_dict(comment)
                instance = Review(**clean_comment)
                business.reviews.append(instance)

                text = clean_comment['text']
                if text is not None:
                    text1 = text.replace(';', ' ')
                    text2 = text1.replace(',', ' ')
                    clean_comment['text'] = text2

                self.COMMENTS.append(clean_comment)
            logger.info('Completed comments collection')

        self.collected_businesses.append(business)
        self.create_files(business, filename)

        if not is_loop:
            self.is_running = False
            self.driver.quit()
        else:
            self.COMMENTS = []
            self.collected_businesses = []
            return True

    def iterate_urls(self, comments_scroll_attempts=None, urls=[]):
        """From a file called `media/google_place_urls.csv` containing a 
        set of Google url places, iterate and extract the comments or the
        business information for each Google Place. This calls `start_spider` 
        in a loop passing the current url"""
        file_path = MEDIA_PATH / 'google_place_urls.csv'
        if urls:
            df = pandas.DataFrame({'url': urls})
        else:
            if file_path.exists():
                logger.info("Loading urls from file: 'google_place_urls.csv'")
                df = pandas.read_csv(
                    MEDIA_PATH / 'google_place_urls.csv',
                    encoding='utf-8'
                )

        if 'url' not in list(df.columns):
            raise ValueError("Your file should contain an 'url' column")

        df['is_duplicate'] = df.duplicated(subset=['url'])
        duplicate_rows = df[df['is_duplicate'] == True]
        if duplicate_rows.url.count() > 0:
            logger.warning(
                f"{duplicate_rows.url.count()} duplicate "
                "urls in your file"
            )

        if 'completed' in df.columns:
            df.loc[df.completed.isna()] = False
            df = df[df['completed'] == False]
        else:
            df['completed'] = False

        logger.info(f"Loaded {df['url'].count()} urls")

        self.driver.maximize_window()

        for item in df.itertuples(name='GooglePlace'):
            try:
                self.start_spider(
                    item.url,
                    is_loop=True,
                    comments_scroll_attempts=comments_scroll_attempts,
                    maximize_window=False
                )
            except Exception as e:
                logger.error(f"Error trying to get url: {item.url}")
                logger.error(e)
                continue
            else:
                df.loc[item.Index, 'completed'] = True
                df[['url', 'completed']].to_csv(file_path, index=False)
                
                time.sleep(random.randrange(20, 40))


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

        self.search_data_path = search_data_path = MEDIA_PATH.joinpath(self.initial_data_file)
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
                self.confusion_pages.append([item.data, self.driver.current_url])
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
                    business_information = self.driver.execute_script(constants.BUSINESS_INFORMATION_SCRIPT)
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Google reviews')

    arguments = sys.argv[1:]
    
    cmd = arguments[0]
    parser.add_argument(
        'name',
        type=str,
        help='The name of the review parser to use',
        choices=['place', 'places', 'searchlinks', 'searchbusinesses']
    )
    parser.add_argument(
        '-f',
        '--folder',
        type=str
    )
    parser.add_argument(
        '-w',
        '--webhook',
        type=str,
        help='The webhook to use in order to send data'
    )
    parser.add_argument(
        '-cwt',
        '--comments-scroll-time',
        type=int
    )
    parser.add_argument(
        '-csa',
        '--comment-scroll-attempts',
        type=int
    )
    
    if cmd != 'searchlinks':
        parser.add_argument(
            'url',
            type=str,
            help='The url to visit'
        )
        parser.add_argument(
            '-n',
            type=bool
        )

    if cmd == 'searchlinks':
        parser.add_argument(
            '-i',
            '--initial-data-file',
            type=str,
            help="Use another data file than the default 'search_data.csv'"
        )

    namespace = parser.parse_args()

    if namespace.comments_scroll_time is not None:
        COMMENTS_SCROLL_WAIT_TIME = namespace.comments_scroll_time

    if namespace.comment_scroll_attempts is not None:
        COMMENTS_SCROLL_ATTEMPTS = namespace.comment_scroll_attempts

    if namespace.name == 'place':
        klass = GooglePlace
    elif namespace.name == 'places':
        klass = GooglePlaces
    elif namespace.name == 'searchlinks':
        klass = SearchLinks

    if namespace.name == 'place' or namespace.name == 'places':
        result = check_url(namespace.name, namespace.url)

        if result:
            instance = klass(output_folder=namespace.folder)
            try:
                instance.start_spider(namespace.url)
            except Exception as e:
                instance.after_fail(exception=e)
                logger.critical(e)
            except KeyboardInterrupt:
                instance.after_fail()
                logger.info('Program stopped')
    elif namespace.name == 'searchlinks':
        instance = klass(
            output_folder=namespace.folder,
            initial_data_file=namespace.initial_data_file
        )
        try:
            instance.start_spider()
        except Exception as e:
            instance.after_fail(exception=e)
            logger.critical(e)
        except KeyboardInterrupt:
            instance.after_fail()
            logger.info('Program stopped')
    else:
        instance = klass(output_folder=namespace.folder)
        try:
            instance.start_spider()
        except Exception as e:
            instance.after_fail(exception=e)
            logger.critical(e)
        except KeyboardInterrupt:
            instance.after_fail()
            logger.info('Program stopped')
