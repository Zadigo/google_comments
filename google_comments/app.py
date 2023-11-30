# from twisted.internet.protocol import ClientFactory
# from twisted.protocols.basic import LineReceiver
# from twisted.web.server import Site
# from twisted.internet import defer
# from twisted.internet.protocol import ProcessProtocol
# from twisted import reactor
# from twisted.web.static import File

# class BaseProtocol(LineReceiver):
#     def __init__(self, address):
#         self.address = address

#     def connectionMade(self):
#         deferred = defer.maybeDeferred(start_spider)

#         def parse_comments(comments):
#             return comments

#         deferred.addCallback(parse_comments)


# class BaseFactory(ClientFactory):
#     def buildProtocol(self, address):
#         return BaseProtocol(address)


import multiprocessing
import os

import quart
from google_comments.maps import GooglePlaces
from quart import websocket
from quart.app import asyncio
from quart_cors import cors

app = quart.Quart(__name__)
app = cors(
    app,
    allow_origin=['http://127.0.0.1:5500', 'http://localhost:8080'],
    allow_headers=['content-type', 'authorization'],
    allow_credentials=True
)
app.config.update(SECRET_KEY=os.getenv('SECRET_KEY'))
# app.logger.addHandler(default_handler.get_handler())


class Broker:
    ACTIVE_INSTANCES = set()

    async def create(self, spider):
        self.ACTIVE_INSTANCES.add(spider)

    async def delete(self, spider):
        self.ACTIVE_INSTANCES.remove(spider)


@app.websocket('/ws/start')
async def start():
    queue = multiprocessing.Queue()
    await websocket.accept()

    while True:
        data = await websocket.receive_json()
        if data['method'] == 'scrap':
            # broker = Broker()

            try:
                # instance = start_process(data['url'], queue)
                instance = GooglePlaces(websocket=websocket)
                # broker.create(instance)
                instance.start_spider(data['url'])
            except Exception as e:
                print(e)
            pass


@app.websocket('/ws/parse')
async def parse():
    pass


if __name__ == '__main__':
    app.run('localhost', port='5467', debug=True)
