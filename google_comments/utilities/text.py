import string
import pathlib
import pandas
import unidecode
from google_comments import DATA_PATH

def slugify(text):
    """Returns a slug version of a string for
    instance:
    
    >>> slugify('my business')
    ... "my-business"
    """
    text = unidecode.unidecode(str(text))
    text = text.translate(text.maketrans('', '', string.punctuation))
    lowered_text = text.lower()
    return lowered_text.replace(' ', '-')


class GenerateSearchResults:
    """Generate search values to be used in
    the search bar of Google Map"""

    def __init__(self, search):
        df = pandas.read_csv(DATA_PATH.joinpath('departements.csv'))
        df = df[['name']]
        df['search'] = search + ' near ' + df['name']
        self.dataframe = df

    def __repr__(self):
        return f'<{self.__class__.__name__} searches: {self.dataframe.search.count()}>'

    def __iter__(self):
        for item in self.dataframe.itertuples(name='Search'):
            yield item.search

    def __getitem__(self, index):
        df = self.dataframe.iloc[index]
        return df['search'].to_list()

    def write_file(self):
        self.dataframe.to_csv('search_data.csv', index=False)
