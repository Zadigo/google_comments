# instance = SearchLinks()
# try:
#     instance.start_spider()
# except KeyboardInterrupt:
#     instance.create_file(prefix='dump')
# except Exception as e:
#     instance.create_file(prefix='dump')
#     logger.error(e)

# instance = GooglePlace(output_folder='concurrents_aprium2')
# instance.iterate_urls()

# instance = SearchBusinesses(output_folder='concurrence_aprium')
# try:
#     instance.start_spider()
# except Exception as e:
#     instance.after_fail(exception=e)

# if __name__ == '__main__':
#     parser = create_argument_parser()
#     try:
#         namespace = parser.parse_args()
#     except Exception:
#         raise

#     if namespace.name == 'place':
#         klass = GooglePlace
#     elif namespace.name == 'places':
#         klass = GooglePlaces

#     result = check_url(namespace.name, namespace.url)
#     if result:
#         try:
#             instance = klass()

#             if namespace.collect_reviews:
#                 instance.collect_reviews = namespace.collect_reviews

#             instance.start_spider(namespace.url)
#         except Exception as e:
#             logger.critical(e)
#         except KeyboardInterrupt:
#             logger.info('Program stopped')

# if __name__ == '__main__':
#     parser = argparse.ArgumentParser('Google reviews')
#     parser.add_argument(
#         'name',
#         type=str,
#         help='The name of the review parser to user',
#         choices=['place', 'places']
#     )
#     parser.add_argument(
#         'url',
#         type=str,
#         help='The url to visit'
#     )
#     parser.add_argument(
#         '-w',
#         '--webhook',
#         type=str,
#         help='Webhook to send data'
#     )
#     parser.add_argument(
#         '-s',
#         '--skip-reviews',
#         type=str,
#         default=True,
#         help='Determines if the crawler should not collect the reviews for the given business'
#     )
#     namespace = parser.parse_args()

#     # parser = create_argument_parser()
#     # namespace = parser.parse_args()

#     if namespace.name == 'place':
#         klass = GooglePlace
#     elif namespace.name == 'places':
#         klass = GooglePlaces

#     result = check_url(namespace.name, namespace.url)
#     if result:
#         try:
#             instance = klass()

#             if namespace.skip_reviews:
#                 instance.collect_reviews = False

#             instance.start_spider(namespace.url)
#         except Exception as e:
#             logger.critical(e)
#         except KeyboardInterrupt:
#             logger.info('Program stopped')
