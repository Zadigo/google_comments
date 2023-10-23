import argparse
import asyncio
import csv
import datetime
import json
import multiprocessing
import pathlib
import random
import secrets
import string
import sys
import time
from collections import defaultdict

import pandas
import pytz
from requests.auth import HTTPBasicAuth
from requests.models import Request
from requests.sessions import Session
from selenium.webdriver.common.by import By

from google_comments import (MEDIA_PATH, clean_dict,
                             get_selenium_browser_instance, get_soup, logger,
                             simple_clean_text, text_parser)
from google_comments.models import Comment, GoogleBusiness

COMMENTS_SCROLL_ATTEMPTS = 30

FEED_SCROLL_ATTEMPTS = 30


class WebhookMixin:
    webhook_urls = []
    session = Session()

    async def get_headers(self, **headers):
        base_headers = {}
        return base_headers | headers

    async def create_requests(self):
        for url in self.webhook_urls:
            request = Request('post',  url=url)
            yield self.session.prepare_request(request)

    async def send_requests(self, *, headers={}, credentials={}):
        authentication = None
        if credentials:
            authentication = HTTPBasicAuth(**credentials)

        async def sender(request):
            try:
                response = self.session.send(request, proxies=[])
            except:
                logger.error(f'Request failed for webhook: {request}')
            else:
                return response

        tasks = []
        prepared_requests = await self.create_requests()
        for request in prepared_requests:
            request.headers = self.get_headers(**headers)
            request.auth = authentication
            tasks.append(asyncio.create_task(sender(request)))
        responses = asyncio.gather(*tasks)


class GooglePlaces(WebhookMixin):
    COMMENTS = []
    collected_businesses = []

    def __init__(self):
        self.temporary_id = secrets.token_hex(5)
        self.is_running = False
        self.start_time = datetime.datetime.now(tz=pytz.UTC)
        self.driver = get_selenium_browser_instance()
        self.websocket = None
        self.seen_urls_outputted = False
        logger.info('Starting spider')

    def __repr__(self):
        return f'<{self.__class__.__name__} [{self.temporary_id}]>'

    def __del__(self):
        try:
            self.driver.quit()
            sys.exit(0)
        except:
            logger.info('Program stopped')

    def __hash__(self):
        return hash((self.temporary_id))

    def flatten(self):
        """Flatten the saved dataclasses to dictionnaries"""
        return [business.as_json() for business in self.collected_businesses]

    def get_dataframe(self):
        data = defaultdict(list)
        for business in self.flatten():
            for comment in business['comments']:
                data['review_id'].append(comment.review_id)
                data['text'].append(comment['text'])
                data['rating'].append(comment['rating'])
                data['period'].append(comment.period)
                data['reviewer_name'].append(comment['reviewer_name'])
                data['reviewer_number_of_reviews'].append(
                    comment['reviewer_number_of_reviews']
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

    def poll(self, socket):
        from quart import websocket
        if not isinstance(socket, websocket):
            return False
        self.websocket = socket

        async def sender():
            if self.websocket is not None:
                await self.websocket.send(self.COMMENTS)
        asyncio.run(sender())

    def send_message(self, message):
        async def sender():
            if self.websocket is not None:
                await self.websocket.send({b'error': message})
        asyncio.run(sender)

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
            logger.error('Comments sorted')
            time.sleep(3)

    def start_spider(self, url):
        self.is_running = True

        filename = f'{secrets.token_hex(5)}'
        urls_seen_file = pathlib.Path(MEDIA_PATH / f'{filename}_urls_seen.csv')
        if not urls_seen_file.exists():
            urls_seen_file.touch()

        self.driver.maximize_window()
        self.driver.get(url)

        script = """
        let el = document.querySelector('form:last-child')
        let button = el && el.querySelector('button')
        button && button.click()
        """
        self.driver.execute_script(script)
        time.sleep(5)

        count = 0
        pixels = 2000
        scrolls = []
        while count < FEED_SCROLL_ATTEMPTS:
            if scrolls:
                if len(scrolls) > 2:
                    if scrolls[-1] == scrolls[-2]:
                        break

            script = f"""
            let el = document.querySelector('[role="feed"]')
            el.scroll(0, {pixels})
            return [ el.scrollTop, el.scrollHeight ]
            """
            current_scroll, feed_height = self.driver.execute_script(script)
            scrolls.append(current_scroll)
            count = count + 1
            time.sleep(2)

            script = """
            let el = document.querySelector('[role="feed"]')
            el.scroll({top: 0, left: 0, behavior: "smooth"})
            return [el.scrollTop, el.scrollHeight]
            """
            current_scroll, feed_height = self.driver.execute_script(script)
            pixels = pixels + 2000

        script = """
        let elements = document.querySelectorAll('div[role="feed"] div:not([class])')
        return elements
        """
        elements = self.driver.execute_script(script)

        number_of_business_cards = len(elements)
        logger.info(f'Found {number_of_business_cards} business cards')

        # Iteration for each business card

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

            with open(MEDIA_PATH / f'{filename}_urls_seen.csv', newline='\n', mode='a') as f:
                writer = csv.writer(f)
                writer.writerow([business.name, business.feed_url])

            logger.info(
                f"Created business: '{business.name} @ {business.address}'"
            )

            # Collect the comments on the page
            tab_list = self.driver.find_elements(
                By.CSS_SELECTOR,
                '*[role="tablist"] button'
            )
            try:
                tab_list[1].click()
            except:
                continue
            time.sleep(2)

            if "'" in business_name:
                business_name = business_name.replace("'", "\\'")

            if '"' in business_name:
                business_name = business_name.replace('"', '\\"')

            # Iteration for each comment

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
                current_scroll, scroll_height = self.driver.execute_script(
                    scroll_bottom_script
                )

                if current_scroll > 0:
                    if current_scroll in last_positions:
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
                logger.debug(
                    f'Completed {count} of {FEED_SCROLL_ATTEMPTS} scrolls')
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
                        review_id: dataReviewId,
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
                instance = Comment(**clean_comment)
                business.comments.append(instance)
                self.COMMENTS.append(clean_comment)
            self.collected_businesses.append(business)

            # self.poll()
            with open(MEDIA_PATH / f'{filename}.json', mode='w') as f:
                json.dump(self.flatten(), f)

            with open(MEDIA_PATH / f'{filename}_comments.json', mode='w') as fp:
                json.dump(self.COMMENTS, fp)

            logger.info(
                f"Completed {iteration_count} of "
                f"{number_of_business_cards} business cards"
            )

        self.is_running = False
        self.driver.quit()



# if __name__ == '__main__':
#     parser = argparse.ArgumentParser('Google Comments')
#     parser.add_argument('url', help='Google maps url', type=str)
#     namespace = parser.parse_args()

#     try:
#         checked_url = check_url(namespace.url)
#         if checked_url:
#             instance = GooglePlaces()
#             instance.start_spider(checked_url)
#             # process = multiprocessing.Process(
#             #     target=instance.start_spider,
#             #     args=[url]
#             # )
#             # process.start()
#             # process.join()
#     except KeyboardInterrupt:
#         logger.info('Program stopped')
