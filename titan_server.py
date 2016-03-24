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


# All commands run in the Monitor/Arbiter context
# This allows us to manage central messaging queues
# The can be accessed by workers using the internal app commands (below)

# User defined rpc commands
@pulsar.command(ack=True)
@asyncio.coroutine
def create_person(request, blob):
    """This add a create person task to the incoming task queue"""
    self = request.actor.app
    # Build goblin object from blob
    request_id = str(uuid.uuid4())
    person = titan_pb2.Person()
    person.ParseFromString(blob)
    name = person.name
    email = person.email
    url = person.url
    task = Person().create(
            name=name, email=email, url=url, request_id=request_id)
    # This is only goblin app api method that needs to be exposed
    yield from self.add_task(request_id, task)
    return request_id


# Generic `API` commands
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
def process_task(request, aid):
    """This asks the monitor to make a call and pass of the result handling
       to a worker."""
    self = request.actor.app
    yield from self.process_task(aid)


@pulsar.command(ack=True)
@asyncio.coroutine
def enqueue_response(request, request_id, response):
    """Put a response on the response queue to be read by the rpc service"""
    LOGGER.info("Enqueued by monitor: {}\naid: {}".format(
        request.actor.is_monitor(), request.actor.aid))
    self = request.actor.app
    yield from self.response_queues[request_id].put(response)


@pulsar.command(ack=True)
@asyncio.coroutine
def process_response(request, request_id, response):
    """This is where we serialize and pass to the response queue"""
    LOGGER.info("Processed by monitor: {}\naid: {}".format(
        request.actor.is_monitor(), request.actor.aid))
    LOGGER.info("RESP: {}".format(response))
    # This is because we send some random strings and of course a None
    if not isinstance(response, str) and response:
        # Can we do some kind of generic mapper here to avoid specifics here????
        person = titan_pb2.Person()
        person.name = response.name
        person.url = response.url
        person.email = response.email
        person.id = response.id
        person.label = response.get_label()
        response = person.SerializeToString()
    request.actor.send('monitor', 'enqueue_response', request_id, response)


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
        connection.setup("ws://localhost:8182", pool_class=Pool,
                         future_class=asyncio.Future, loop=monitor._loop)

    def monitor_stopping(self, monitor):
        loop = monitor._loop
        loop.call_soon(pulsar.ensure_future, connection.tear_down())

    def add_task(self, request_id, task):
        """Add a task to the incoming task queue"""
        self.response_queues[request_id] = asyncio.Queue()
        yield from self.incoming_queue.put((request_id, task))

    @asyncio.coroutine
    def read_response(self, response_id):
        """This method allows the rpc service to read from the response queues
            maintained by the app."""
        try:
            queue = self.response_queues[response_id]
        except KeyError:
            raise KeyError("Bad response_id")
        else:
            resp = yield from queue.get()
            if resp is None:
                del self.response_queues[response_id]
            return resp

    def worker_start(self, worker, exc=None):
        """Worker starts prompting the monitor to process tasks and pass them
           off for post processing..."""
        self._loop = worker._loop
        # check the queue periodically for tasks...
        self._loop.call_later(1, self.start_working, worker)

    def start_working(self, worker):
        """Don't be lazy"""
        pulsar.ensure_future(self.run(worker))

    @asyncio.coroutine
    def run(self, worker):
        """Try to get tasks from the monitor. Prompt processing, read from
           streaming results. Push read for client results into the appropriate
           response queue."""
        # Tell the monitor to dequeue and iterate task
        # If the monitor has no tasks to process, it returns None
        pulsar.ensure_future(worker.send(worker.monitor, 'process_task', worker.aid))
        worker._loop.call_later(1, self.start_working, worker)

    @asyncio.coroutine
    def process_task(self, aid):
        """Check for tasks, if available, pass results to calling worker,
            worker pushes responses into queue...."""
        try:
            request_id, task = self.incoming_queue.get_nowait()
        except asyncio.QueueEmpty:
            LOGGER.info("No tasks available :( :( :(")
        else:
            resp = yield from task
            if isinstance(resp, gremlinclient.connection.Stream):
                # Farm out the processing to the worker process that
                # called the process task command
                while True:
                    msg = yield from resp.read()
                    yield from pulsar.send(
                        aid, 'process_response', request_id, resp)
                    if msg is None:
                        break
            else:
                # Farm out the processing to the worker process that
                # called the process task command
                yield from pulsar.send(
                    aid, 'process_response', request_id, "hello")
                yield from asyncio.sleep(1)
                yield from pulsar.send(
                    aid, 'process_response', request_id, "world")
                yield from asyncio.sleep(1)
                yield from pulsar.send(
                    aid, 'process_response', request_id, resp)
                yield from asyncio.sleep(1)
                yield from pulsar.send(
                    aid, 'process_response', request_id, None)


class CreatorRPC(WSRPC):

    def rpc_create_person(self, websocket, blob):
        request_id = yield from pulsar.send(
            'goblin_server', 'create_person', blob)
        while True:
            blob = yield from pulsar.send(
                'goblin_server', 'read_response', request_id)
            if blob is None:
                break
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
