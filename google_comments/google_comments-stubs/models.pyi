import datetime
from ctypes import Union
from dataclasses import dataclass, field
from functools import cached_property
from typing import Any, Optional
from urllib.parse import ParseResult

import pandas
import pytz

class BaseModel:
    def __getitem__(self, key: str) -> Any: ...
    def __setitem__(self, key: str, value: Any) -> None: ...

    @cached_property
    def fields(self) -> list[str]: ...
    @cached_property
    def get_url_object(self) -> ParseResult: ...
    @cached_property
    def url_stem(self) -> str: ...

    def as_json(self) -> dict[str, Any]: ...


@dataclass
class Review(BaseModel):
    google_review_id: Optional[str] = None
    text: Optional[str] = None
    rating: Optional[str] = None
    period: Optional[str] = None
    reviewer_name: Optional[str] = None
    reviewer_number_of_reviews: Optional[str] = None

    def __hash__(self) -> int: ...


@dataclass
class GoogleBusiness(BaseModel):
    name: Optional[str] = None
    scrap_id: Optional[str] = None
    url_business_id: Optional[str] = None
    url: Optional[str] = None
    feed_url: Optional[str] = None
    address: Optional[str] = None
    rating: Optional[str] = None
    latitude: Optional[int] = None
    longitude: Optional[int] = None
    number_of_reviews: int = None
    date: Optional[datetime.datetime] = field(default_factory=lambda: datetime.datetime.now(tz=pytz.UTC))
    business_type: Optional[str] = None
    permanently_closed: Optional[bool] = False
    additional_information: Optional[str] = None
    telephone: Optional[str] = None
    website: Optional[str] = None
    reviews: Optional[list] = field(default_factory=list)

    def as_csv(self) -> list: ...
    def as_dataframe(self) -> pandas.DataFrame: ...

    def get_gps_coordinates_from_feed_url(
        self
    ) -> Union[bool, list[str, str]]: ...

    def get_gps_coordinates_from_url(self, substitute_url=None) -> Union[bool, str]: ...


@dataclass
class SearchedBusinesses(BaseModel):
    search_url: Optional[str] = None
    places: Optional[list] = field(default_factory=list)
    date: Optional[datetime.datetime] = field(default_factory=lambda: datetime.datetime.now(tz=pytz.UTC))
