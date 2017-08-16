from rpcz import rpcz_pb2
import zmq
import zmq.asyncio
import asyncio
from uuid import uuid4

class ReplyChannel:
    def __init__(self, message_headers, socket):
        self.message_headers = message_headers
        self.socket = socket

    async def send(self, message):
        reply_header = rpcz_pb2.rpc_response_header()

        reply_header.status = rpcz_pb2.rpc_response_header.OK

        reply_header_raw = reply_header.SerializeToString()
        message_raw = message.SerializeToString()

        msg = self.message_headers + [reply_header_raw, message_raw]
        await self.socket.send_multipart(msg)

    async def send_error(self, error, error_msg=None):
        reply_header = rpcz_pb2.rpc_response_header()
        reply_header.application_error = error

        if error_msg:
            reply_header.error = error_msg

        reply_header_raw = reply_header.SerializeToString()

        msg = self.message_headers + [reply_header_raw,]
        await self.socket.send_multipart(msg)


class AsyncRpczServerMeta(type):
    def __new__(cls, name, bases, attrs):
        if "DESCRIPTOR" not in attrs:
            return super(AsyncRpczServerMeta, cls).__new__(cls, name, bases, attrs)
        descriptor = attrs["DESCRIPTOR"]
        attrs["_service_name"] = descriptor.name
        attrs["_method_descriptor_map"] = {method.name: method for method in descriptor.methods}
        return super(AsyncRpczServerMeta, cls).__new__(cls, name, bases, attrs)

class AsyncRpczServer(metaclass=AsyncRpczServerMeta):
    def __init__(self, number_of_workers=10):
        self._backend_address = "inproc://.{}".format(uuid4())
        self._ctx = zmq.asyncio.Context()
        self._number_of_workers = number_of_workers

    async def backend_worker(self, worker_address):
        ctx = self._ctx
        backend_socket = ctx.socket(zmq.PAIR)
        backend_socket.linger = 0
        backend_socket.bind(worker_address)

        while True:
            header = rpcz_pb2.rpc_request_header()

            msg = await backend_socket.recv_multipart()

            message_headers = msg[:-2]

            header_raw = msg[-2]
            msg_raw = msg[-1]

            reply = ReplyChannel(message_headers, backend_socket)

            try:
                header.ParseFromString(header_raw)
            except:
                await reply.send_error(rpcz_pb2.rpc_response_header.INVALID_HEADER)
                continue

            if header.service != self._service_name:
                await reply.send_error(rpcz_pb2.rpc_response_header.NO_SUCH_SERVICE)
                continue

            method_name = header.method

            method_descriptor = self._method_descriptor_map.get(method_name, None)
            if method_descriptor is None:
                await reply.send_error(rpcz_pb2.rpc_response_header.NO_SUCH_METHOD)
                continue

            method = getattr(self, method_name, None)
            if method is None:
                await reply.send_error(rpcz_pb2.rpc_response_header.METHOD_NOT_IMPLEMENTED)
                continue

            msg = method_descriptor.input_type._concrete_class()

            try:
                msg.ParseFromString(msg_raw)
            except:
                await reply.send_error(rpcz_pb2.rpc_response_header.INVALID_MESSAGE)
                continue

            await method(msg, reply)

            await backend_socket.send(b"")

    async def frontend_worker(self, server_address):
        ctx = self._ctx
        frontend_socket = ctx.socket(zmq.ROUTER)
        frontend_socket.linger = 0

        if isinstance(server_address, str):
            server_address = [server_address,]

        for address in server_address:
            frontend_socket.bind(address)

        poller = zmq.asyncio.Poller()
        poller.register(frontend_socket, flags=zmq.POLLIN)

        free_backend_sockets = set()
        socket_to_address = {}
        for worker_address in self.workers_addresses:
            backend_socket = ctx.socket(zmq.PAIR)
            backend_socket.connect(worker_address)
            poller.register(backend_socket, flags=zmq.POLLIN)
            free_backend_sockets.add(backend_socket)
            socket_to_address[backend_socket] = worker_address

        busy_backend_sockets = set()

        while True:
            socket_events = await poller.poll(10)

            for socket, _ in socket_events:
                if socket is frontend_socket:
                    if len(free_backend_sockets) == 0:
                        await frontend_socket.recv_multipart()
                        continue

                    backend_socket = free_backend_sockets.pop()
                    busy_backend_sockets.add(backend_socket)

                    msg = await frontend_socket.recv_multipart()
                    await backend_socket.send_multipart(msg)
                elif socket in busy_backend_sockets:
                    msg = await socket.recv_multipart()

                    if len(msg) == 1:
                        busy_backend_sockets.remove(socket)
                        free_backend_sockets.add(socket)
                    else:
                        await frontend_socket.send_multipart(msg)


    async def run(self, server_address):
        self.workers_addresses = ["inproc://worker_{}".format(uuid4()) for worker_id in range(self._number_of_workers)]

        backend_workers_tasks = [self.backend_worker(worker_address) for worker_address in self.workers_addresses]
        frontend_worker_task = self.frontend_worker(server_address)

        tasks = backend_workers_tasks
        tasks.append(frontend_worker_task)
        await asyncio.gather(*tasks)


