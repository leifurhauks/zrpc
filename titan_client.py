import asyncio

from proto import titan_pb2
from wsrpc import WSRPCProxy


@asyncio.coroutine
def main():
    proxy = WSRPCProxy('http://127.0.0.1:8060')
    person = titan_pb2.Person()
    person.name = "jon"
    person.email = "jon@jon.com"
    person.url = "https://jon.com/"
    blob = person.SerializeToString()
    client = yield from proxy.create_person(blob)
    try:
        while True:
            resp = yield from client.receive()
            print(resp.data)
            if resp.data == "CLOSE":
                break
    finally:
        yield from client.close()
        yield from proxy.session.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
