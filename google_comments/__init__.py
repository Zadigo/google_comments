import dataclasses
import datetime
import logging
import pathlib
import re
import secrets

import dotenv
import pytz
import unidecode
from bs4 import BeautifulSoup
from selenium.webdriver import Edge, EdgeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.proxy import Proxy, ProxyType
from webdriver_manager.microsoft import EdgeChromiumDriverManager

PROXY_IP_ADDRESS = None

PROJECT_PATH = pathlib.Path(__file__).parent.absolute()

MEDIA_PATH = PROJECT_PATH / 'media'

dotenv.load_dotenv(PROJECT_PATH / '.env')


def simple_clean_text(text):
    if text is None:
        return text
    else:
        text = unidecode.unidecode(str(text).strip())
        tokens = text.split(' ')
        return ' '.join(filter(lambda x: x != '', tokens))


def clean_dict(item):
    def clean_value(value):
        text = value.encode('utf-8').decode()
        text = text.replace('\n', '')
        return simple_clean_text(text)

    if dataclasses.is_dataclass(item):
        for field in item.fields:
            item[field] = clean_value(item[field])
        return item
    else:
        new_dict = {}
        for key, value in item.items():
            if value is None:
                new_dict[key] = value
            else:
                new_dict[key] = clean_value(value)
        return new_dict


class Logger:
    instance = None

    def __init__(self, name='GOOGLE MAPS'):
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)

        handler = logging.StreamHandler()
        logger.addHandler(handler)

        log_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M'
        )
        handler.setFormatter(log_format)

        file_handler = logging.FileHandler(PROJECT_PATH / 'access.log')
        logger.addHandler(file_handler)
        file_handler.setFormatter(log_format)

        self.instance = logger

    @classmethod
    def create(cls, name):
        instance = cls(name=name)
        return instance

    def warning(self, message, *args, **kwargs):
        self.instance.warning(message, *args, **kwargs)

    def info(self, message, *args, **kwargs):
        self.instance.info(message, *args, **kwargs)

    def error(self, message, *args, **kwargs):
        self.instance.error(message, *args, **kwargs)

    def debug(self, message, *args, **kwargs):
        self.instance.debug(message, *args, **kwargs)

    def critical(self, message, *args, **kwargs):
        self.instance.critical(message, *args, **kwargs)


logger = Logger()


def get_selenium_browser_instance(headless=False, load_images=True, load_js=True):
    options = EdgeOptions()
    options.add_argument('--remote-allow-origins=*')
    # options.add_argument(f'--user-agent=')
    options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})

    if headless:
        options.headless = True

    # 0 = Default, 1 = Allow, 2 = Block
    preferences = {
        'profile.default_content_setting_values': {
            'images': 0 if load_images else 2,
            'javascript': 0 if load_js else 2,
            'popups': 2,
            'geolocation': 2,
            'notifications': 2
        }
    }
    options.add_experimental_option('prefs', preferences)

    # Proxies
    if PROXY_IP_ADDRESS is not None:
        proxy = Proxy()
        proxy.proxy_type = ProxyType.MANUAL
        proxy.http_proxy = PROXY_IP_ADDRESS
        options.add_argument(f'--proxy-server=http://{PROXY_IP_ADDRESS}')
        options.add_argument('--disable-gpu')

    service = Service(EdgeChromiumDriverManager().install())
    return Edge(service=service, options=options)


def get_soup(content):
    return BeautifulSoup(content, 'html.parser')


def text_parser(text):
    text = unidecode.unidecode(text)
    if text == '' or text is None:
        return ''

    if isinstance(text, int):
        return text

    if 'avis' in text:
        result = text.replace('avis', '').strip().replace(' ', '')
        return int(result)

    if 'etoiles' in text:
        result = re.match(r'(\d+\,?\d+)\s?etoiles', text)
        if result:
            text = result.group(1)
            return float(text.replace(',', '.'))
    return text


def check_url(spider_type, url):
    if spider_type == 'place' and '/maps/place/' not in url:
        logger.error(
            f"url is not valid for {spider_type}. "
            "Url should contain /maps/place/"
        )
        return False

    if spider_type == 'places' and '/maps/search/' not in url:
        logger.error(
            f"url is not valid for {spider_type}. "
            "Url should contain /maps/search/"
        )
        return False
    return True


def create_filename(*, prefix=None, suffix=None, include_date=True):
    filename = secrets.token_hex(nbytes=5)

    if prefix is not None:
        filename = f'{prefix}_{filename}'

    if suffix is not None:
        filename = f'{filename}_{suffix}'

    if include_date:
        current_date = datetime.datetime.now(
            tz=pytz.UTC).date().strftime('%Y-%m-%d %H:%M')
        date_string = str(current_date).replace(' ', '-').replace(':', '_')
        filename = f'{filename}_{date_string}'
    return filename


async def write_json_file(filename, data):
    with open(MEDIA_PATH / filename, mode='w', encoding='utf-8') as f:
        json.dump(data, f)
