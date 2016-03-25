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

Get and run Titan:db:
```
$ wget http://s3.thinkaurelius.com/downloads/titan/titan-1.0.0-hadoop1.zip
$ unzip titan-1.0.0-hadoop1.zip
$ cd titan-1.0.0-hadoop1/
$ ./bin/titan.sh start
```

Install Python package dependencies:

```
$ pip install pip install git+https://github.com/ZEROFAIL/goblin.git@dev#egg=goblin
$ pip install aiohttp pulsar protobuf==3.0.0b2.post2
```
Finally, clone this repo and navigate to the root so you can run the examples:

```
$ git clone https://github.com/davebshow/zrpc.git
$ cd zrpc/
```

### WSRPC

Ok. Pulsar gives you the components to easily build and WebSocket RPC handler. Here is a simple [example](https://github.com/davebshow/zrpc/blob/master/wsrpc.py#L78):

```python
import asyncio
import struct

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
        websocket.write_close()


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

import aiohttp

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
            if resp.tp == aiohttp.MsgType.close:
                break
            print(resp.data)
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
$ cd zrpc/
$ python echo_client.py
```

### Using WSRPC w/Titan and Protocol Buffers

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

...
