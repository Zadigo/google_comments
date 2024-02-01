import argparse

from google_comments import check_url, create_argument_parser, logger
from google_comments.maps import GooglePlace, GooglePlaces

if __name__ == '__main__':
    parser = argparse.ArgumentParser('Google reviews')
    parser.add_argument(
        'name', 
        type=str, 
        help='The name of the review parser to user', 
        choices=['place', 'places']
    )
    parser.add_argument(
        'url', 
        type=str, 
        help='The url to visit'
    )
    parser.add_argument(
        '-w',
        '--webhook', 
        type=str,
        help='Webhook to send data'
    )
    parser.add_argument(
        '-c',
        '--collect-reviews',
        type=bool,
        default=True,
        help='Determines if the crawler should collect the reviews for the given business'
    )
    namespace = parser.parse_args()

    # parser = create_argument_parser()
    # namespace = parser.parse_args()

    if namespace.name == 'place':
        klass = GooglePlace
    elif namespace.name == 'places':
        klass = GooglePlaces

    result = check_url(namespace.name, namespace.url)
    if result:
        try:
            instance = klass()
   
            if namespace.collect_reviews:
                instance.collect_reviews = namespace.collect_reviews

            instance.start_spider(namespace.url)
        except Exception as e:
            logger.critical(e)
        except KeyboardInterrupt:
            logger.info('Program stopped')
