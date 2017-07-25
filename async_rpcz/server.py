from rpcz import rpcz_pb2
import zmq
import zmq.asyncio
import asyncio

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
    async def run(self, server_address):
        ctx = zmq.asyncio.Context()
        socket = ctx.socket(zmq.REP);
        socket.bind(server_address)

        while True:
            header = rpcz_pb2.rpc_request_header()

            event_id, header_raw, msg_raw = await socket.recv_multipart()
            reply = ReplyChannel(event_id, socket)

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

