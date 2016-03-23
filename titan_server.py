import asyncio
import pulsar
from pulsar import get_actor
from pulsar.apps import wsgi, ws
from pulsar.apps.wsgi.utils import LOGGER
from gremlinclient.aiohttp_client import Pool
from goblin import connection
from models import Person
from proto import titan_pb2
from wsrpc import WSRPC


@pulsar.command(ack=True)
@asyncio.coroutine
def create_person(request, blob):
    self = request.actor.app
    return (yield from self.create_person(blob))


class Goblin(pulsar.Application):

    def monitor_start(self, monitor):
        connection.setup("ws://localhost:8182", pool_class=Pool,
                         future_class=asyncio.Future, loop=monitor._loop)

    def monitor_stopping(self, monitor):
        loop = monitor._loop
        loop.call_soon(pulsar.ensure_future, connection.tear_down())

    @asyncio.coroutine
    def create_person(self, blob):
        LOGGER.info("METHOD: {}".format(blob))
        person = titan_pb2.Person()
        person.ParseFromString(blob)
        name = person.name
        email = person.email
        url = person.url
        # This is the goblin model Person
        resp = yield from Person().create(name=name, email=email, url=url)
        LOGGER.info("Created: {}".format(resp))
        # Add the id and label
        new_person = titan_pb2.Person()
        new_person.name = resp.name
        new_person.url = resp.url
        new_person.email = resp.email
        new_person.id = resp.id
        new_person.label = resp.get_label()
        return new_person.SerializeToString()


class CreatorRPC(WSRPC):

    def rpc_create_person(self, websocket, blob):
        blob = yield from pulsar.send('goblin_server', 'create_person', blob)
        LOGGER.info("Created outgoing blob: {}".format(blob))
        websocket.write(blob)
        websocket.write_close()


class TitanRPCSite(wsgi.LazyWsgi):
    """Handler for the RPCServer"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


    def setup(self, environ):
        wm = ws.WebSocket('/', CreatorRPC())
        return wsgi.WsgiHandler(middleware=[wm], async=True)


class Server(pulsar.MultiApp):

    @asyncio.coroutine
    def build(self):
        yield self.new_app(wsgi.WSGIServer, callable=TitanRPCSite())
        yield self.new_app(Goblin, name='goblin')


if __name__ == "__main__":
    Server().start()
