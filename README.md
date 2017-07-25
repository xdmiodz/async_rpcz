## Python implementation of RPCZ server/client with asyncio support

[RPCZ](https://github.com/thesamet/rpcz) is great, it mixes both two useful technologies - ZeroMQ and Protobuf. But the main project is abandoned and it lacks of support for another very useful thing in Python - asyncio. So I decided to make an alternative implementation of RPCZ client/server in Python, but this time with asyncio.

Currently it has only server implementation, but I hope there will be the client too.
Now the implemation is kind of cumbersome, but it works


## Usage
Let's suppose you have a `my_service.proto` file with description of your service:

```
message Void {
};

service MyService {
    rpc SayHello(Void) returns(Void);
};

```

The `rpcz` compiler makes a file named `my_service_rpcz.py` from the description. Here is how you can use it with the library.

### Server

```python
from async_rpcz import AsyncRpczServer
from . import my_service_rpcz
from . import my_service_pb2


class MyService(AsyncRpczServer):
    DESCRIPTOR = my_service_rpcz._MYSERVICE

    async def SayHello(self, request, reply):
        print("Hello world")
        respone = my_service_pb2.Void()
        await reply.send(response)
```

You can start the server using  the `run` coroutine. As an argument it accepts any zmq-correct address. For example:

```
from .server import MyService
import asyncio
import zmq
import zmq.asyncio

loop = zmq.asyncio.ZMQEventLoop()
asyncio.set_event_loop(loop)

def main():
    server = MyService()
    server_task = server.run("tcp://127.0.0.1:9000")
    loop.run_until_complete(server_task)

```
