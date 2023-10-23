import argparse

from google_comments.maps import GooglePlaces

from google_comments import check_url, logger

if __name__ == '__main__':
    parser = argparse.ArgumentParser('Google Comments')
    parser.add_argument('url', help='Google maps url', type=str)
    namespace = parser.parse_args()

    try:
        checked_url = check_url(namespace.url)
        if checked_url:
            instance = GooglePlaces()
            instance.start_spider(checked_url)
            # process = multiprocessing.Process(
            #     target=instance.start_spider,
            #     args=[url]
            # )
            # process.start()
            # process.join()
    except KeyboardInterrupt:
        logger.info('Program stopped')
