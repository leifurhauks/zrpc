import asyncio
import struct

import aiohttp

from pulsar import as_coroutine, ensure_future, AsyncObject
from pulsar.apps import rpc, ws
from pulsar.apps.wsgi.utils import LOGGER


class WSCall:
    slots = ('_client', '_name')

    def __init__(self, client, name):
        self._client = client
        self._name = name

    def __repr__(self):
        return self._name
    __str__ = __repr__

    @property
    def url(self):
        return self._client.url

    @property
    def name(self):
        return self._name

    def __getattr__(self, name):
        name = "%s%s%s" % (self._name, name)
        return self.__class__(self._client, name)

    def __call__(self, *args, **kwargs):
        return self._client._call(self._name, *args, **kwargs)


class WSRPCProxy(AsyncObject):

    def __init__(self, url, loop=None, **kwargs):
        self._url = url
        self._session = aiohttp.ClientSession(loop=loop)

    @property
    def url(self):
        return self._url

    @property
    def session(self):
        return self._session

    @property
    def _loop(self):
        return self._session._loop

    def makeid(self):
        '''Can be re-implemented by your own Proxy'''
        return gen_unique_id()

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self.__url)

    def __str__(self):
        return self.__repr__()

    def __getattr__(self, name):
        return WSCall(self, name)

    def _call(self, name, blob):
        method = name.encode("utf-8")
        data = struct.pack(
            "@I%ds%ds" % (len(method), len(blob)), len(method), method, blob)
        client = yield from self._session.ws_connect(self._url)
        client.send_bytes(data)
        return client


class WSRPC(rpc.handlers.RpcHandler, ws.WS):

    def on_bytes(self, websocket, body):
        execute = self._execute(websocket, body)
        websocket._loop.call_soon(ensure_future, execute)

    @asyncio.coroutine
    def _execute(self, websocket, body):
        yield from self._call(websocket, body)

    @asyncio.coroutine
    def _call(self, websocket, body):
        # Process subprotocol
        (i, ), data = struct.unpack("I", body[:4]), body[4:]
        method, blob = data[:i], data[i:]
        LOGGER.info("method: {}\nblob: {}".format(method, blob))
        proc = self.get_handler(method.decode('utf-8'))
        yield from as_coroutine(proc(websocket, blob))
