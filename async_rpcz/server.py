from rpcz import rpcz_pb2
import zmq
import zmq.asyncio
import asyncio
from uuid import uuid4

class ReplyChannel:
    def __init__(self, event_id, socket):
        self.event_id = event_id
        self.socket = socket

    async def send(self, message):
        reply_header = rpcz_pb2.rpc_response_header()

        reply_header.status = rpcz_pb2.rpc_response_header.OK

        reply_header_raw = reply_header.SerializeToString()
        message_raw = message.SerializeToString()
        await self.socket.send_multipart([self.event_id, reply_header_raw, message_raw])

    async def send_error(self, error, error_msg=None):
        reply_header = rpcz_pb2.rpc_response_header()
        reply_header.application_error = error
        if error_msg:
            reply_header.error = error_msg

        reply_header_raw = reply_header.SerializeToString()
        await self.socket.send_multipart([self.event_id, reply_header_raw])

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

    async def backend_worker(self):
        ctx = self._ctx
        backend_socket = ctx.socket(zmq.REP)
        backend_socket.connect(self._backend_address)

        while True:
            header = rpcz_pb2.rpc_request_header()

            event_id, header_raw, msg_raw = await backend_socket.recv_multipart()
            reply = ReplyChannel(event_id, backend_socket)

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

    async def frontend_worker(self, server_address):
        ctx = self._ctx
        frontend_socket = ctx.socket(zmq.ROUTER)
        frontend_socket.bind(server_address)

        backend_socket = ctx.socket(zmq.DEALER)
        backend_socket.bind(self._backend_address)

        poller = zmq.asyncio.Poller()
        poller.register(frontend_socket, flags=zmq.POLLIN)
        poller.register(backend_socket, flags=zmq.POLLIN)

        while True:
            socket_events  = dict(await poller.poll(10))
            for socket in socket_events:
                if socket is frontend_socket:
                    msg = await frontend_socket.recv_multipart()
                    await backend_socket.send_multipart(msg)
                else:
                    msg = await backend_socket.recv_multipart()
                    await frontend_socket.send_multipart(msg)

    async def run(self, server_address):
        backend_workers_tasks = [self.backend_worker() for _ in range(self._number_of_workers)]
        frontend_worker_task = self.frontend_worker(server_address)

        tasks = backend_workers_tasks
        tasks.append(frontend_worker_task)
        await asyncio.gather(*tasks)


