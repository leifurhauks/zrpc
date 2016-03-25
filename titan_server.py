import asyncio
import uuid
import pulsar
from pulsar import get_actor
from pulsar.apps import wsgi, ws
from pulsar.apps.wsgi.utils import LOGGER
import gremlinclient
from gremlinclient.aiohttp_client import Pool
from goblin import connection
from models import Person
from proto import titan_pb2
from wsrpc import WSRPC


# Simple example. This will have to be more carefully handled -
# mananging serializer names etc....
@asyncio.coroutine
def create_person(request_id, blob):
    person = titan_pb2.Person()
    person.ParseFromString(blob)
    name = person.name
    email = person.email
    url = person.url
    person = yield from Person().create(
            name=name, email=email, url=url, request_id=request_id)
    return person


def serialize_person(response):
    try:
        person = titan_pb2.Person()
        person.name = response.name
        person.url = response.url
        person.email = response.email
        person.id = response.id
        person.label = response.get_label()
        response = person.SerializeToString()
    finally:
        return response


# Simple example. This will have to be more carefully handled.
command_map = {
    "create_person": create_person,
    "serialize_person": serialize_person}


# `API` commands
@pulsar.command(ack=True)
@asyncio.coroutine
def add_task(request, method, blob):
    """Add the data from the rpc to the incoming task queue"""
    self = request.actor.app
    request_id = str(uuid.uuid4())
    yield from self.add_task(request_id, method, blob)
    return request_id


@pulsar.command(ack=True)
@asyncio.coroutine
def read_response(request, request_id):
    """Called by rpc service to stream incoming results"""
    self = request.actor.app
    blob = yield from self.read_response(request_id)
    return blob


#Internal app commands - how workers communicate with the monitor process
# read from queues etc.
@pulsar.command(ack=True)
@asyncio.coroutine
def get_task(request):
    """Called by worker process to try to get a task from the monitor
       controlled main incoming task queue"""
    self = request.actor.app
    return self.get_task()


@pulsar.command(ack=True)
@asyncio.coroutine
def enqueue_response(request, request_id, response):
    """Called by a worker to put a response on the monitor controlled response
       queue to be read by the rpc service"""
    LOGGER.info("Enqueued by monitor: {}\naid: {}".format(
        request.actor.is_monitor(), request.actor.aid))
    LOGGER.info(response)
    self = request.actor.app
    yield from self.response_queues[request_id].put(response)


@pulsar.command(ack=True)
@asyncio.coroutine
def process_task(request, request_id, method, blob):
    """This implements streaming task processing using user defined
       functions to perform tasks like serialization. Enqueues processed tasks
       response on the monitor controlled response_queues"""
    LOGGER.info("Processed by monitor: {}\naid: {}".format(
        request.actor.is_monitor(), request.actor.aid))
    # Simple example. This will have to be more carefully handled
    serializer = "serialize_{}".format(method.split('_')[-1])
    try:
        resp = yield from command_map[method](request_id, blob)
    except KeyError:
        raise KeyError("Unknown command issued")
    else:
        if isinstance(resp, gremlinclient.connection.Stream):
            while True:
                msg = yield from resp.read()
                if msg:
                    msg = command_map[serializer](msg)
                yield from request.actor.send(
                    'monitor', 'enqueue_response', request_id, msg)
                if msg is None:
                    break
        else:
            try:
                msg = command_map[serializer](resp)
            except KeyError:
                raise KeyError("Unregistered serializer")
            else:
                yield from request.actor.send(
                    'monitor', 'enqueue_response', request_id, msg)

                # Demonstrate streaming
                yield from asyncio.sleep(1)
                yield from request.actor.send(
                    'monitor', 'enqueue_response', request_id, "Hello")
                yield from asyncio.sleep(1)
                yield from request.actor.send(
                    'monitor', 'enqueue_response', request_id, "Streaming")
                yield from asyncio.sleep(1)
                yield from request.actor.send(
                    'monitor', 'enqueue_response', request_id, "World")

                # Message for client to terminate
                yield from request.actor.send(
                    'monitor', 'enqueue_response', request_id, None)


class Goblin(pulsar.Application):

    cfg = pulsar.Config(workers=2)

    def monitor_start(self, monitor):
        """Setup message queues. Setup goblin"""
        # This lives in the monitor context
        # Queue incoming messages from rpc service
        self.incoming_queue = asyncio.Queue(maxsize=250)
        # These queues hold response data that can be asynchronously read
        # by the rpc service
        self.response_queues = {}

    def add_task(self, request_id, method, blob):
        """Add a task to the incoming task queue"""
        self.response_queues[request_id] = asyncio.Queue()
        yield from self.incoming_queue.put((request_id, method, blob))

    @asyncio.coroutine
    def read_response(self, request_id):
        """This method allows the rpc service to read from the response queues
            maintained by the app."""
        try:
            queue = self.response_queues[request_id]
        except KeyError:
            raise KeyError("Bad request id")
        else:
            resp = yield from queue.get()
            if resp is None:
                del self.response_queues[request_id]
            return resp

    def worker_start(self, worker, exc=None):
        """Setup the global goblin variables, then start asking the monitor
           for tasks..."""
        connection.setup("ws://localhost:8182", pool_class=Pool,
                         future_class=asyncio.Future, loop=worker._loop)
        # check the queue periodically for tasks...
        worker._loop.call_soon(self.start_working, worker)

    def worker_stopping(self, worker, exc=None):
        """Close the connection pool for this process"""
        worker._loop.call_soon(pulsar.ensure_future, connection.tear_down())

    def start_working(self, worker):
        """Don't be lazy"""
        pulsar.ensure_future(self.run(worker))

    @asyncio.coroutine
    def run(self, worker):
        """Try to get tasks from the monitor. If tasks are available process
           using same worker, if not, wait a second, and ask again...
           BE PERSISTENT!"""
        request_id, method, blob = yield from worker.send(
            worker.monitor, 'get_task')
        if request_id and method and blob:
            yield from pulsar.send(
                worker.aid, 'process_task', request_id, method, blob)
        worker._loop.call_later(1, self.start_working, worker)

    def get_task(self):
        """Check for tasks, if available, pass data to calling worker for
           processing..."""
        try:
            request_id, method, blob = self.incoming_queue.get_nowait()
        except asyncio.QueueEmpty:
            LOGGER.info("No tasks available :( :( :(")
            return None, None, None
        else:
            return request_id, method, blob


class TitanRPC(WSRPC):

    def rpc_dispatch(self, websocket, method, blob):
        """A generic dispatch function that sends commands to the
           Goblin application"""
        request_id = yield from pulsar.send(
            'goblin_server', 'add_task', method, blob)
        while True:
            blob = yield from pulsar.send(
                'goblin_server', 'read_response', request_id)
            if blob is None:
                break
            websocket.write(blob)
        websocket.write_close()


class TitanRPCSite(wsgi.LazyWsgi):
    """Handler for the RPCServer"""

    def setup(self, environ):
        wm = ws.WebSocket('/', TitanRPC())
        return wsgi.WsgiHandler(middleware=[wm], async=True)


class Server(pulsar.MultiApp):

    @asyncio.coroutine
    def build(self):
        yield self.new_app(wsgi.WSGIServer, callable=TitanRPCSite())
        yield self.new_app(Goblin, name='goblin')


if __name__ == "__main__":
    Server().start()
