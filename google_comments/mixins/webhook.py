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


class WebhookMixin:
    webhook_urls = []
    session = Session()

    async def get_headers(self, **headers):
        base_headers = {'content-type': 'application/json'}
        return base_headers | headers

    async def create_requests(self, data={}):
        for url in self.webhook_urls:
            request = Request(method='post',  url=url, json=data)
            yield self.session.prepare_request(request)

    async def send_requests(self, *, data={}, headers={}, credentials={}):
        authentication = None
        if credentials:
            authentication = HTTPBasicAuth(**credentials)

        async def sender(request):
            try:
                response = self.session.send(request, proxies=[])
            except Exception as e:
                logger.error(f'Request failed for webhook: {request.path_url}')
                logger.error(' / '.join(e.args))
            else:
                return response

        tasks = []
        # prepared_requests = await
        async for request in self.create_requests(data=data):
            request.headers = await self.get_headers(**headers)
            request.auth = authentication
            tasks.append(asyncio.create_task(sender(request)))

        for task in asyncio.as_completed(tasks):
            response = await task

    def trigger_webhooks(self, *, data={}, **kwargs):
        """Send data to the registered webhooks"""
        async def main():
            await self.send_requests(data=data, **kwargs)
        asyncio.run(main())
