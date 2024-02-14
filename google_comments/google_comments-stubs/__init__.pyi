import argparse
import pathlib
from logging.config import Logger as TrueLogger
from typing import Any, Union
from webbrowser import Chrome

from bs4 import BeautifulSoup
from selenium.webdriver import Edge

PROXY_IP_ADDRESS: str

PROJECT_PATH: pathlib.Path

DATA_PATH: pathlib.Path

MEDIA_PATH: pathlib.Path


def simple_clean_text(text, remove_accents=True) -> str: ...


def clean_dict(item: Union[list[dict[str, Any]], dict[str, Any]]) -> dict: ...


class Logger:
    instance: TrueLogger = ...

    def __init__(self, name: str = ...) -> None: ...

    @classmethod
    def create(cls, name: str) -> Logger: ...

    def warning(self, message: str, *args, **kwargs) -> None: ...
    def info(self, message: str, *args, **kwargs) -> None: ...
    def error(self, message: str, *args, **kwargs) -> None: ...
    def debug(self, message: str, *args, **kwargs) -> None: ...
    def critical(self, message: str, *args, **kwargs) -> None: ...


logger = Logger()


def get_selenium_browser_instance(
    headless: bool = ..., 
    load_images: bool = ..., 
    load_js: bool = ...
) -> Union[Edge, Chrome]: ...


def get_soup(content: str) -> BeautifulSoup: ...


def text_parser(text: str) -> str: ...


def check_url(spider_type: str, url: str) -> bool: ...


def create_filename(
    *, 
    prefix: str = ..., 
    suffix: str = ...,
    include_date: bool = ...
) -> str: ...


async def write_json_file(filename, data) -> None: ...


def create_argument_parser() -> argparse.ArgumentParser: ...


def clean_raw_information(text) -> str: ...
