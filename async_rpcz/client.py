from rpcz.rpc import RpcDeadlineExceeded, RpcApplicationError, RpcError
from rpcz import rpcz_pb2
import zmq
import zmq.asyncio
import asyncio
import uuid
import struct
from .async_timeout import timeout
from asyncio import Lock

class AsyncRpczClient:
    def __init__(self, server_address, descriptor):
        self._service_name = descriptor.name
        self._method_descriptor_map = {method.name: method for method in descriptor.methods}

        self._ctx = zmq.asyncio.Context()
        self._backend_socket = self._ctx.socket(zmq.DEALER)
        self._backend_socket.linger = 0
        self._backend_socket.connect(server_address)
        self._events = dict()
        self._event_id = 0
        self._socket_lock = Lock()

    @property
    def event_id(self):
        self._event_id =+ 1
        return self._event_id

    async def _process(self):
        async with self._socket_lock:
            msg = await self._backend_socket.recv_multipart()

        _, event_id_raw, header_raw, msg_raw = msg
        event_id = struct.unpack("!Q", event_id_raw)[0]
        self._events[event_id] = True
        return header_raw, msg_raw

    def __getattr__(self, name):
        method_descriptor = self._method_descriptor_map.get(name, None)
        if method_descriptor is None:
            raise AttributeError("No rpcz method {}".format(name))

        async def _method(request, deadline_ms=-1):
            event_id = self.event_id
            header = rpcz_pb2.rpc_request_header()
            header.event_id = event_id
            header.deadline = deadline_ms
            header.service = self._service_name
            header.method = name

            event_id_raw = struct.pack("!Q", event_id)
            header_raw = header.SerializeToString()
            request_raw = request.SerializeToString()

            event = self._events[event_id] = False
            async with self._socket_lock:
                await self._backend_socket.send_multipart([b"", event_id_raw, header_raw, request_raw])

            deadline_ms = deadline_ms if deadline_ms >= 0 else None

            try:
                with timeout(deadline_ms):
                    while not self._events[event_id]:
                        header_raw, msg_raw = await self._process()
            except asyncio.TimeoutError:
                raise RpcDeadlineExceeded()

            self._events.pop(event_id)
            header = rpcz_pb2.rpc_response_header()
            header.ParseFromString(header_raw)

            if header.status == rpcz_pb2.rpc_response_header.APPLICATION_ERROR:
                raise RpcApplicationError(header.application_error, header.error)
            elif header.status != rpcz_pb2.rpc_response_header.OK:
                raise RpcError(header.status)

            response = method_descriptor.output_type._concrete_class()
            response.ParseFromString(msg_raw)
            return response

        return _method

