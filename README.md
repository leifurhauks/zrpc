## WebSocket RPC w/Google Protocol Buffers (proto3)

This example demonstrates how you can use Pulsar to create a websocket based streaming RPC service and proxy that is easy to integrate with data serialized to bytes e.g., Google Protocol Buffers.

### Setup

Install Google Protocol Buffers:

```
$ wget https://github.com/google/protobuf/releases/download/v3.0.0-beta-2/protobuf-python-3.0.0-beta-2.tar.gz
$ tar -xvzf protobuf-python-3.0.0-beta-2.tar.gz
$ cd protobuf-3.0.0-beta-2/
$ ./configure
$ make
```
Go make some coffee....
```
$ make check
$ sudo make install
```
Check that you have protoc 3
```
$ protoc --version
```
If you get a weird error, you may need to run
```
$ sudo ldconfig
```
Phew, good to go, now...

Install Python package dependencies:

```
$ pip install pip install git+https://github.com/ZEROFAIL/goblin.git@dev#egg=goblin
$ pip install aiohttp pulsar protobuf==3.0.0b2.post2
```
Finally, clone this repo and navigate to the root so you can run the examples:

```
$ git clone https://github.com/davebshow/zrpc.git
$ cd zrpc
```

### WSRPC

Ok. Pulsar gives you the components to easily build and WebSocket RPC handler. Here is a simple [example](https://github.com/davebshow/zrpc/blob/master/wsrpc.py#L78):

```python
import asyncio
import struct

import aiohttp

from pulsar import as_coroutine, ensure_future
from pulsar.apps import rpc, ws
from pulsar.apps.wsgi.utils import LOGGER


class WSRPC(rpc.handlers.RpcHandler, ws.WS):

    def on_bytes(self, websocket, body):
        execute = self._execute(websocket, body)
        websocket._loop.call_soon(ensure_future, execute)

    @asyncio.coroutine
    def _execute(self, websocket, body):
        yield from self._call(websocket, body)

    @asyncio.coroutine
    def _call(self, websocket, body):
        # Use a simple subprotocol
        (i, ), data = struct.unpack("I", body[:4]), body[4:]
        method, blob = data[:i], data[i:]
        LOGGER.info("method: {}\nblob: {}".format(method, blob))
        proc = self.get_handler(method.decode('utf-8'))
        yield from as_coroutine(proc(websocket, blob))
```

### WSRCP Echo Server

You can then inherit from this class to create a custom WSRPC handler. Here is a simple echo server:

```python
import asyncio

from pulsar.apps import rpc, wsgi, ws

from wsrpc import WSRPC


class EchoRPC(WSRPC):

    def rpc_echo(self, websocket, blob):
        # Streams the echo 10 times
        for x in range(10):
            yield from asyncio.sleep(0.5)
            websocket.write(blob)
        websocket.write('CLOSE')


def server():
    wm = ws.WebSocket('/', EchoRPC())
    app = wsgi.WsgiHandler(middleware=[wm])
    return wsgi.WSGIServer(callable=app)


if __name__ == "__main__":
    server().start()
```

This service can be called from any websocket client implementation in any language. Python clients can use the [``WSRCPProxy``](https://github.com/davebshow/zrpc/blob/master/wsrpc.py#L38) to call the rpc methods. Underneath, the proxy uses the ``aiohttp`` to communicate with the server, because Pulsar doesn't include as websocket client out of the box:

```python
import asyncio

from wsrpc import WSRPCProxy


@asyncio.coroutine
def client():
    proxy = WSRPCProxy('http://127.0.0.1:8060')
    # WSRPC methods accept any binary blob
    client = yield from proxy.echo(b"world")
    try:
        # Basic websocket read pattern...
        while True:
            resp = yield from client.receive()
            print(resp.data)
            if resp.data == "CLOSE":
                break
    # Clean up
    finally:
        yield from client.close()
        yield from proxy.session.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(client())
    finally:
        loop.close()
```

To run this example, start the server:

```
$ python echo_server.py
```

Then open a new terminal and navigate to the zrpc root and run the client:

```
$ cd zrpc
$ python echo_client.py
```

### Using WSRPC with Titan and Protocol Buffers

So let's set up a service that creates vertices of type ``Person`` in Titan:db. Since we will be using Protocol Buffers for serialization, let's define a simple message type person in a `.proto` file called `titan.proto`:

```
syntax = "proto3";

message Person {
  string name = 1;
  string email = 2;
  string url = 3;
  int32 id = 4;
  string label = 5;
}
```

This file can be compiled doing something like this, but you don't need to do it because it has already been done:

```
$ protoc --python_out=proto/ titan.proto
```

The file it generates is called `titan_pb2`, which is misleading. Upon checking the [source](https://github.com/davebshow/zrpc/blob/master/proto/titan_pb2.py) though, you can see it is indeed using the proto3 syntax.

Next we will define a WSRPC Handler that creates a ``Person``, and then creates several more while streaming the results back for effect. We then we serve using a WSGI server with some simple ``goblin`` specific hooks:

```python
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
        # Parse the blob, a serialized protobuf
        person = titan_pb2.Person()
        person.ParseFromString(blob)
        name = person.name
        email = person.email
        url = person.url

        # Create a person node in the db
        resp = yield from Person().create(name=name, email=email, url=url)
        LOGGER.info("Created: {}".format(resp))

        # Add the id and label to the protobuf
        person.id = resp.id
        person.label = resp.get_label()

        # Serialize and write protobuf
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
            # Build protobuf, serialize, and write to wire
            person = titan_pb2.Person()
            person.name = resp.name
            person.email = resp.name
            person.url = resp.email
            person.id = resp.id
            person.label = resp.get_label()
            person.SerializeToString()
            websocket.write(blob)

        # More random streaming and sleeping
        for x in range(10):
            websocket.write("Sleeping")
            yield from asyncio.sleep(0.5)
            websocket.write(str(x))
        websocket.write("GOODBYE")
        yield from asyncio.sleep(1)

        # Close command
        websocket.write("CLOSE")


class TitanRPCSite(wsgi.LazyWsgi):
    """Handler for the RPCServer"""

    def __init__(self):
        """Setup goblin"""
        setup("localhost", pool_class=Pool, future=asyncio.Future)

    def setup(self, environ):
        """Add websocket middleware to handler"""
        wm = ws.WebSocket('/', CreatorRPC())
        return wsgi.WsgiHandler(middleware=[wm])


class TitanRPCServer(wsgi.WSGIServer):

    def monitor_stopping(self, monitor):
        """Add hook to close goblin pool on shutdown"""
        loop = monitor._loop
        loop.call_soon(ensure_future, close_global_pool())


def server(callable=None, **params):
    return TitanRPCServer(TitanRPCSite(), **params)


if __name__ == "__main__":
    server().start()
```

We can then create a simple client similar to the echo client, but with protobuf:


```python
import asyncio

from proto import titan_pb2
from wsrpc import WSRPCProxy


@asyncio.coroutine
def main():
    proxy = WSRPCProxy('http://127.0.0.1:8060')
    # Build a protobuf, serialize, and path to proxy method
    person = titan_pb2.Person()
    person.name = "jon"
    person.email = "jon@jon.com"
    person.url = "https://jon.com/"
    blob = person.SerializeToString()
    client = yield from proxy.create_person(blob)
    # Basic websocket read pattern...
    try:
        while True:
            resp = yield from client.receive()
            print(resp.data)
            if resp.data == "CLOSE":
                break
    # Clean up
    finally:
        yield from client.close()
        yield from proxy.session.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()

```

To run this example, start the server:

```
$ python titan_server.py
```

Then open a new terminal and navigate to the zrpc root and run the client:

```
$ cd zrpc
$ python titan_client.py
```

That's it! These are really basic examples, but they can be as complex as needed...

Node.js client example coming soon...
