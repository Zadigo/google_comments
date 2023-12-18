import argparse

from google_comments import check_url, logger
from google_comments.maps import GooglePlaces

if __name__ == '__main__':
    parser = argparse.ArgumentParser('Google reviews')
    parser.add_argument(
        'name', 
        type=str, 
        help='The name of the review parser to user', 
        choices=['place', 'places']
    )
    parser.add_argument('url', type=str, help='The url to visit')
    parser.add_argument(
        '-w',
        '--webhook', 
        type=str,
        help='Webhook to send data'
    )
    namespace = parser.parse_args()

    if namespace.name == 'place':
        klass = GooglePlace
    elif namespace.name == 'places':
        klass = GooglePlaces

    result = check_url(namespace.name, namespace.url)
    if result:

        try:
            instance = klass()
            # instance.webhook_urls = ['http://127.0.0.1:8000/api/v1/google-comments/review/bulk']
            instance.start_spider(namespace.url)
        except Exception as e:
            logger.critical(e)
        except KeyboardInterrupt:
            logger.info('Program stopped')
