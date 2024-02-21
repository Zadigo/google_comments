import asyncio
import datetime

import pytz
import redis
from google_comments.maps import GooglePlace

from google_comments import logger


class BaseServer:
    def __init__(self):
        self.started = False
        self.db = None
        db = redis.Redis(
            host='localhost',
            port=6379,
            decode_responses=True
        )
        try:
            db.ping()
        except:
            logger.error('Could not get redis database')
        else:
            self.db = db
            self.started = True


base_server = BaseServer()


def get_date():
    tz = pytz.timezone('Europe/Paris')
    return datetime.datetime.now(tz=tz)


async def run_server():
    logger.info('Starting server...')

    instance = GooglePlace()
    instance.collect_reviews = False

    next_execution_date = get_date()
    interval = datetime.timedelta(minutes=1)

    while True:
        current_date = get_date()

        if current_date > next_execution_date:
            next_execution_date = next_execution_date + interval

            if base_server.started:
                has_url = base_server.db.exists('url')
                if has_url:
                    logger.info('Running task...')
                    url = base_server.db.getdel('url')
                    instance.iterate_urls(urls=[url])
            else:
                logger.warning('Server could not be started')

        await asyncio.sleep(10)


if __name__ == '__main__':
    try:
        asyncio.run(run_server())
    except KeyboardInterrupt:
        logger.info('Server stopped')
