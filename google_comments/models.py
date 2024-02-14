import dataclasses
import datetime
import pathlib
import re
from collections import defaultdict
from dataclasses import dataclass, field
from functools import cached_property
from urllib.parse import unquote, urlparse

import pandas
import pytz


class BaseModel:
    """Base class for all models"""

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        setattr(self, key, value)

    @cached_property
    def fields(self):
        """Get the fields present on the model"""
        fields = dataclasses.fields(self)
        return list(map(lambda x: x.name, fields))

    @cached_property
    def get_url_object(self):
        result = unquote(getattr(self, 'url', ''))
        return urlparse(str(result))

    @cached_property
    def url_stem(self):
        return pathlib.Path(str(self.url)).stem

    def as_json(self):
        pass


@dataclass
class Review(BaseModel):
    google_review_id: str = None
    text: str = None
    rating: str = None
    period: str = None
    reviewer_name: str = None
    reviewer_number_of_reviews: str = None

    def __hash__(self):
        return hash((self.google_review_id))

    def as_json(self):
        return {field: getattr(self, field) for field in self.fields}


@dataclass
class GoogleBusiness(BaseModel):
    """This is the base model for creating a new
    Google business JSON item for data
    standardization/consistency purposes
    
    >>> data = {'name': 'Zara'}
    ... item = GoogleBusiness(**data)
    """
    
    name: str = None
    scrap_id: str = None
    url_business_id: str = None
    url: str = None
    feed_url: str = None
    address: str = None
    rating: str = None
    latitude: int = None
    longitude: int = None
    number_of_reviews: int = None
    date: str = field(default=datetime.datetime.now(tz=pytz.UTC))
    business_type: str = None
    permanently_closed: bool = False
    additional_information: str = None
    telephone: str = None
    website: str = None
    reviews: str = field(default_factory=list)

    def __hash__(self):
        return hash((self.name, self.url))

    def as_csv(self):
        rows = []
        for review in self.reviews:
            row = [
                self.name, self.url, self.address, self.rating,
                self.number_of_reviews, review['period'],
                review['text']
            ]
            rows.append(row)
        header = [*self.fields, 'review_period', 'review_text']
        return rows.insert(0, header)

    def as_json(self):
        data = {
            **{field: getattr(self, field) for field in self.fields},
            'reviews': [review.as_json() for review in self.reviews]
        }
        data['date'] = str(data['date'])
        return data

    def as_dataframe(self):
        data = defaultdict(list)
        for review in self.reviews:
            data['google_review_id'].append(review.google_review_id)
            data['text'].append(review.text)
            data['rating'].append(review.rating)
            data['period'].append(review.period)
            data['reviewer_name'].append(review.reviewer_name)
            data['reviewer_number_of_reviews'].append(
                review.reviewer_number_of_reviews
            )

            data['date'].append(self.date)
            data['name'].append(self.name)
            data['url'].append(self.url)
            data['feed_url'].append(self.feed_url)
            data['address'].append(self.address)
            data['company_rating'].append(self.rating)
            data['latitude'].append(self.latitude)
            data['longitude'].append(self.longitude)
            data['number_of_reviews'].append(self.number_of_reviews)
        return pandas.DataFrame(data)

    def get_gps_coordinates_from_feed_url(self):
        try:
            result = re.findall(
                r'(\-?\d+\.\-?\d+)',
                self.feed_url
            )
        except:
            return False
        else:
            if result:
                self.latitude = result[0]
                self.longitude = result[1]
                return result
            return False
            
    def get_gps_coordinates_from_url(self, substitute_url=None):
        try:
            result = re.search(
                r'\@(?P<lat>\d+\.?\d+)\,?(?P<long>\-?\d+\.?\d+)',
                self.url
            )
        except:
            return False
        else:
            if result:
                data = result.groupdict()
                self.latitude = data['lat']
                self.longitude = data['long']
                return data
            

@dataclass
class SearchedBusinesses(BaseModel):
    search_url: str = None
    places: list = field(default_factory=list)
    date: str = field(default=datetime.datetime.now(tz=pytz.UTC))

    def as_json(self):
        data = {
            **{field: getattr(self, field) for field in self.fields},
            'places': [place.as_json() for place in self.places]
        }
        return data
