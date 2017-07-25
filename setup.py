from setuptools import setup, find_packages

setup(
    name = "async_rpcz",
    version = "0.1",
    author = "Dmitry Odzerikho",
    author_email = "dmitry.odzerikho@gmail.com",
    description = "Async implementation of RPCZ for Python",
    keywords = "rpcz asyncio protobuf zmq",
    packages=find_packages(),
)
