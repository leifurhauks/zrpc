import asyncio

from pulsar import ensure_future
from pulsar.apps import wsgi, ws
from pulsar.apps.wsgi.utils import LOGGER

from goblin.connection import setup, close_global_pool
from gremlinclient.aiohttp import Pool

from proto import titan_pb2
from models import Person
from wsrpc import WSRPC


class CreatorRPC(WSRPC):

    def rpc_create_person(self, websocket, blob):
        # Do some extra work to demonstrate the streaming capabilities
        person = titan_pb2.Person()
        person.ParseFromString(blob)
        name = person.name
        email = person.email
        url = person.url
        LOGGER.info(name)
        # This is the goblin model Person
        resp = yield from Person().create(name=name, email=email, url=url)
        LOGGER.info("Created: {}".format(resp))
        # Add the id and label
        person.id = resp.id
        person.label = resp.get_label()
        blob = person.SerializeToString()
        websocket.write(blob)
        # Let's create some more and stream them back
        nodes = [
            {"name": "dave", "email": "dave@dave.com", "url": "https://dave.com/"},
            {"name": "leif", "email": "leif@leif.com", "url": "https://leif.com/"}]
        websocket.write("Let's stream....")
        for node in nodes:
            websocket.write("Sleeping...")
            yield from asyncio.sleep(2)
            websocket.write("Create a node...")
            resp = yield from Person().create(name=node["name"],
                                              email=node["email"],
                                              url=node["url"])
            person = titan_pb2.Person()
            person.name = resp.name
            person.email = resp.name
            person.url = resp.email
            person.id = resp.id
            person.label = resp.get_label()
            person.SerializeToString()
            websocket.write(blob)
        for x in range(10):
            websocket.write("Sleeping")
            yield from asyncio.sleep(0.5)
            websocket.write(str(x))
        websocket.write("GOODBYE")
        yield from asyncio.sleep(1)
        websocket.write_close()


class TitanRPCSite(wsgi.LazyWsgi):
    """Handler for the RPCServer"""

    def __init__(self):
        setup("localhost", pool_class=Pool, future=asyncio.Future)

    def setup(self, environ):
        wm = ws.WebSocket('/', CreatorRPC())
        return wsgi.WsgiHandler(middleware=[wm])


class TitanRPCServer(wsgi.WSGIServer):

    def monitor_stopping(self, monitor):
        loop = monitor._loop
        loop.call_soon(ensure_future, close_global_pool())


def server(callable=None, **params):
    return TitanRPCServer(TitanRPCSite(), **params)


if __name__ == "__main__":
    server().start()
