import dataclasses
import pathlib
import pytz
import re
import datetime
from collections import defaultdict
from dataclasses import dataclass, field
from functools import cached_property
from urllib.parse import unquote, urlparse

import pandas


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


@dataclass
class Comment(BaseModel):
    review_id: str = None
    text: str = None
    rating: str = None
    period: str = None
    reviewer_name: str = None
    reviewer_number_of_reviews: str = None

    def __hash__(self):
        return hash((self.review_id))

    def as_json(self):
        return {field: getattr(self, field) for field in self.fields}


@dataclass
class GoogleBusiness(BaseModel):
    name: str = None
    url: str = None
    feed_url: str = None
    address: str = None
    rating: str = None
    latitude: int = None
    longitude: int = None
    number_of_reviews: int = None
    date: str = field(default=datetime.datetime.now(tz=pytz.UTC))
    additional_information: str = None
    telephone: str = None
    website: str = None
    comments: str = field(default_factory=list)

    def __hash__(self):
        return hash((self.name, self.url))

    def as_csv(self):
        rows = []
        for comment in self.comments:
            row = [
                self.name, self.url, self.address, self.rating,
                self.number_of_reviews, comment['period'],
                comment['text']
            ]
            rows.append(row)
        header = [*self.fields, 'comment_period', 'comment_text']
        return rows.insert(0, header)

    def as_json(self):
        data = {
            **{field: getattr(self, field) for field in self.fields},
            'comments': [comment.as_json() for comment in self.comments]
        }
        data['date'] = str(data['date'])
        return data

    def as_dataframe(self):
        data = defaultdict(list)
        for comment in self.comments:
            data['review_id'].append(comment.review_id)
            data['text'].append(comment.text)
            data['rating'].append(comment.rating)
            data['period'].append(comment.period)
            data['reviewer_name'].append(comment.reviewer_name)
            data['reviewer_number_of_reviews'].append(
                comment.reviewer_number_of_reviews
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

    def get_gps_coordinates_from_url(self, substitute_url=None):
        try:
            result = re.search(
                r'\@(\d+\.?\d+)\,?(\d+\.?\d+)',
                substitute_url or self.feed_url
            )
        except:
            return False
        else:
            if result:
                self.latitude = result.group(1)
                self.longitude = result.group(2)
                return result.groups()
