import argparse
import asyncio
import csv
import datetime
import json
import os
import pathlib
import random
import re
import secrets
import string
import sys
import time
from collections import Counter, defaultdict

import pandas
import pytz
from requests.auth import HTTPBasicAuth
from requests.models import Request
from requests.sessions import Session
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

from google_comments import (MEDIA_PATH, check_url, clean_dict, constants,
                             create_argument_parser, create_filename,
                             get_selenium_browser_instance, get_soup, logger,
                             models, simple_clean_text, text_parser)
from google_comments.models import GoogleBusiness, Review
from google_comments.utilities import encoders, file_helpers
from google_comments.utilities.file_helpers import write_csv_file
from google_comments.utilities.text import slugify


class SpiderMixin:
    """Global base functionnalities that implemented
    for a Google Maps spider"""

    collected_businesses = []

    def __init__(self, output_folder=None):
        self.output_folder = output_folder
        self.output_folder_path = MEDIA_PATH
        self.is_running = False
        self.start_time = datetime.datetime.now(tz=pytz.UTC)
        self.comments_scroll_counter = Counter()
        self.feed_scroll_counter = Counter()

        if output_folder is not None:
            self.output_folder_path = self.output_folder_path.joinpath(
                output_folder)
            if not self.output_folder_path.exists():
                self.output_folder_path.mkdir()

    @property
    def is_feed_page(self):
        """Checks whether the current page contains a feed"""
        return self.driver.execute_script(constants.IS_FEED_PAGE_SCRIPT)

    @property
    def current_page_url(self):
        """Function that returns the page url. This is
        designed to get the url of a dynamic page aka
        a page that changes url but without refreshing
        the page hence making the change invisible to
        the selenium driver"""
        current_page_url_script = """
        return window.location.href
        """
        return self.driver.execute_script(current_page_url_script)

    def flatten(self):
        """Flatten the data (transforms it from a dataclass to a dict)
        using the specified method under this function"""

    def before_launch(self):
        """Custom actions to be run before selenium
        starts executing actions on the current page"""

    def current_page_actions(self):
        """Custom additional actions to run on the
        current page"""

    def after_fail(self, exception=None):
        """Code to execute once the spider has failed
        due to an exception"""

    def completion_percentage(self, total_count):
        return f'{round(self.current_page_url / total_count, 1)}%'

    def check_address(self, model, address):
        pass

    def test_current_scroll_repetition(self, counter, current_scroll, limit=3):
        # When the current_scroll is in the last
        # three positions, we can safely break
        # the looop otherwise we'll have to
        # to the max of COMMENTS_SCROLL_ATTEMPTS
        result = counter[current_scroll]
        if result >= limit:
            return True
        return False

    def scroll_feed(self):
        """Function used to scroll a Google Maps feed. A feed
        is the side section of Google Maps that proposes multiple
        business places to select from"""
        count = 0
        pixels = 2000
        # scrolls = []
        while count < FEED_SCROLL_ATTEMPTS:
            # if scrolls:
            #     if len(scrolls) > 2:
            #         if scrolls[-1] == scrolls[-2]:
            #             break
            try:
                script = f"""
                let el = document.querySelector('[role="feed"]')
                el.scroll(0, {pixels})
                return [ el.scrollTop, el.scrollHeight ]
                """
                current_scroll, feed_height = self.driver.execute_script(
                    script
                )
            except Exception as e:
                logger.error("Could not scroll feed or feed does not exist")
                logger.error(e)
                break
            else:
                # scrolls.append(current_scroll)
                self.feed_scroll_counter.update({current_scroll: 1})
                count = count + 1

                if self.test_current_scroll_repetition(self.feed_scroll_counter, current_scroll, limit=2):
                    self.feed_scroll_counter.clear()
                    break

                time.sleep(2)

            try:
                script = """
                let el = document.querySelector('[role="feed"]')
                el.scroll({top: 0, left: 0, behavior: "smooth"})
                return [ el.scrollTop, el.scrollHeight ]
                """
                current_scroll, feed_height = self.driver.execute_script(
                    script
                )
            except Exception as e:
                logger.error("Could not scroll feed or feed does not exist")
                logger.error(e)
                break
            else:
                pixels = pixels + 2000

    def click_consent(self):
        """Function that clicks on the cookie 
        consent button"""
        script = """
        let el = document.querySelector('form:last-child')
        let button = el && el.querySelector('button')
        button && button.click()
        """
        try:
            self.driver.execute_script(script)
        except:
            return False
        else:
            time.sleep(5)
            return True
