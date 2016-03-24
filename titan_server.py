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
    self = request.actor.app
    # Build goblin object from blob
    request_id = str(uuid.uuid4())
    person = titan_pb2.Person()
    person.ParseFromString(blob)
    name = person.name
    email = person.email
    url = person.url

    @asyncio.coroutine
    def task():
        # Task needs to return a string, so they response can go to the
        # calling worker for postprocessing.
        # Maybe goblin would benefit from sometime returning a raw Server
        # response that could later be dealt with...the worker could then
        # take care of parsing and creating a protobuf blob
        resp = yield from Person().create(
            name=name, email=email, url=url, request_id=request_id)
        LOGGER.info("Created node: {}".format(resp))
        new_person = titan_pb2.Person()
        new_person.name = resp.name
        new_person.url = resp.url
        new_person.email = resp.email
        new_person.id = resp.id
        new_person.label = resp.get_label()
        blob = new_person.SerializeToString()
        return blob

    task = pulsar.ensure_future(task())
    # This is only goblin app api method that needs to be exposed
    yield from self.add_task(request_id, task)
    return request_id


# Generic `API` commands
@pulsar.command(ack=True)
@asyncio.coroutine
def read_response(request, resp_id):
    self = request.actor.app
    blob = yield from self.read_response(resp_id)
    return blob


#Internal app commands - how workers communicate with the monitor process
# read from queues etc.
@pulsar.command(ack=True)
@asyncio.coroutine
def process_task(request):
    self = request.actor.app
    return (yield from self.process_task())


@pulsar.command(ack=True)
@asyncio.coroutine
def queue_response(request, request_id, resp):
    self = request.actor.app
    yield from self.queue_response(request_id, resp)


@pulsar.command(ack=True)
@asyncio.coroutine
def read_processing_queue(request):
    self = request.actor.app
    return (yield from self.processing_queue.get())


class Goblin(pulsar.Application):

    cfg = pulsar.Config(workers=1)

    def monitor_start(self, monitor):
        """Setup message queues. Setup goblin"""
        # This lives in the monitor context
        # Queue incoming messages from rpc service
        self.incoming_queue = asyncio.Queue(maxsize=250)
        # This queue is used to store intemediate results streamed from
        # the gremlin server as they are received but before they are
        # processed
        self.processing_queue = asyncio.Queue()
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
        # Tell the monitor to deque and iterate task
        # If the monitor has no tasks to process, it returns None
        resp = yield from pulsar.send(worker.monitor, 'process_task')
        if resp:
            while True:
                request_id, resp = yield from pulsar.send(
                    worker.monitor, 'read_processing_queue')
                if resp is None:
                    # Send, None, the Terminator
                    yield from pulsar.send(
                        worker.monitor, 'queue_response', request_id, resp)
                    break
                # Do work in worker process...
                # ...
                # Send the results back to monitor queue so rpc client can read
                yield from pulsar.send(
                    worker.monitor, 'queue_response', request_id, resp)
        # check the queue periodically for tasks...
        worker._loop.call_later(1, self.start_working, worker)

    @asyncio.coroutine
    def process_task(self):
        """Check for tasks, if available, begin streaming results to a response
           queue...."""
        try:
            request_id, task = self.incoming_queue.get_nowait()
        except asyncio.QueueEmpty:
            LOGGER.info("No tasks available :( :( :(")
        else:
            resp = yield from task
            if isinstance(resp, gremlinclient.connection.Stream):
                while True:
                    msg = yield from resp.read()
                    yield from self.processing_queue.put((request_id, msg))
                    if msg is None:
                        break
            else:
                yield from self.processing_queue.put((request_id, resp))
                yield from self.processing_queue.put((request_id, None))
            return True

    @asyncio.coroutine
    def queue_response(self, request_id, response):
        """Send a response to the appropriate client response queue"""
        queue = self.response_queues[request_id]
        # For fun to show streaming
        yield from queue.put("hello")
        yield from queue.put("world")
        yield from queue.put(response)


class CreatorRPC(WSRPC):

    def rpc_create_person(self, websocket, blob):
        request_id = yield from pulsar.send(
            'goblin_server', 'create_person', blob)
        while True:
            blob = yield from pulsar.send(
                'goblin_server', 'read_response', request_id)
            if blob is None:
                break
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
