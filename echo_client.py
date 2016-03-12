import asyncio

import aiohttp

from wsrpc import WSRPCProxy


@asyncio.coroutine
def client():
    proxy = WSRPCProxy('http://127.0.0.1:8060')
    client = yield from proxy.echo(b"world")
    try:
        while True:
            resp = yield from client.receive()
            if resp.tp == aiohttp.MsgType.close:
                break
            print(resp.data)
    finally:
        yield from client.close()
        yield from proxy.session.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(client())
    finally:
        loop.close()
