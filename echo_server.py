import asyncio

from pulsar.apps import rpc, wsgi, ws

from wsrpc import WSRPC


class EchoRPC(WSRPC):

    def rpc_echo(self, websocket, blob):
        for x in range(10):
            yield from asyncio.sleep(0.5)
            websocket.write(blob)
        websocket.write('CLOSE')


def server():
    wm = ws.WebSocket('/', EchoRPC())
    app = wsgi.WsgiHandler(middleware=[wm])
    wsgi.WSGIServer(callable=app).start()


if __name__ == "__main__":
    server().start()
