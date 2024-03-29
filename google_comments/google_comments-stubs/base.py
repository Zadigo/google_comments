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
from selenium.webdriver import Edge, Chrome
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
    collected_businesses: list = ...
    output_folder: pathlib.Path = ...
    output_folder_path: pathlib.Path = ...
    is_running: bool = ...
    start_time: datetime.datetime.now = ...
    comments_scroll_counter: Counter = ...
    feed_scroll_counter: Counter = ...

    def __init__(self, output_folder: str = None) -> None: ...

    @property
    def is_feed_page(self) -> bool: ...
    @property
    def current_page_url(self) -> str: ...

    def flatten(self) -> Union[list, dict]: ...
    def before_launch(self) -> None: ...
    def current_page_actions(self) -> None: ...
    def after_fail(self, exception: str = ...) -> None: ...
    def completion_percentage(self, total_count) -> str: ...
    def check_address(self, model, address) -> None: ...

    def test_current_scroll_repetition(
        self,
        counter: Counter,
        current_scroll: int,
        limit: int = ...
    ) -> bool: ...
    def scroll_feed(self) -> None: ...
    def click_consent(self) -> bool: ...

    def start_spider(self, url: str): ...
