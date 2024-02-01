import string

import unidecode


def slugify(text):
    """Returns a slug version of a string"""
    text = unidecode.unidecode(str(text))
    text = text.translate(text.maketrans('', '', string.punctuation))
    lowered_text = text.lower()
    return lowered_text.replace(' ', '-')
