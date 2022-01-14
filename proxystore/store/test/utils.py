"""Mocking utilities for GlobusStore tests"""
import socket
import uuid
from typing import Any
from typing import Union

import globus_sdk
import redis
from _pytest.monkeypatch import MonkeyPatch
from parsl.data_provider import globus

from proxystore.store.file import FileFactory
from proxystore.store.file import FileStore
from proxystore.store.globus import GlobusEndpoint
from proxystore.store.globus import GlobusEndpoints
from proxystore.store.globus import GlobusFactory
from proxystore.store.globus import GlobusStore
from proxystore.store.local import LocalFactory
from proxystore.store.local import LocalStore
from proxystore.store.redis import RedisFactory
from proxystore.store.redis import RedisStore

REDIS_HOST = 'localhost'
REDIS_PORT = 59465
FILE_DIR = "/tmp/proxystore-test-298711396448"
MOCK_GLOBUS_ENDPOINTS = GlobusEndpoints(
    GlobusEndpoint(
        uuid='EP1UUID',
        endpoint_path='/~/',
        local_path=FILE_DIR,
        host_regex='localhost',
    ),
    GlobusEndpoint(
        uuid='EP2UUID',
        endpoint_path='/~/',
        local_path=FILE_DIR,
        host_regex='localhost',
    ),
)
LOCAL_STORE = {
    "type": LocalStore,
    "name": "local",
    "kwargs": {},
    "factory": LocalFactory,
}
FILE_STORE = {
    "type": FileStore,
    "name": "file",
    "kwargs": {"store_dir": FILE_DIR},
    "factory": FileFactory,
}
REDIS_STORE = {
    "type": RedisStore,
    "name": "redis",
    "kwargs": {"hostname": REDIS_HOST, "port": REDIS_PORT},
    "factory": RedisFactory,
}
GLOBUS_STORE = {
    "type": GlobusStore,
    "name": "globus",
    "kwargs": {"endpoints": MOCK_GLOBUS_ENDPOINTS},
    "factory": GlobusFactory,
}


class MockTransferClient:
    def __init__(self, *args, **kwargs):
        pass

    def get_task(self, *args, **kwargs):
        return None

    def submit_delete(self, *args, **kwargs):
        return {"task_id": str(uuid.uuid4())}

    def submit_transfer(self, *args, **kwargs):
        return {"task_id": str(uuid.uuid4())}

    def task_wait(self, *args, **kwargs):
        return True


class MockTransferData:
    def __init__(self, *args, **kwargs):
        pass

    def __setitem__(self, key, item):
        self.__dict__[key] = item

    def __getitem__(self, key):
        return self.__dict__[key]

    def add_item(self, *args, **kwargs):
        return


class MockDeleteData:
    def __init__(self, *args, **kwargs):
        pass

    def __setitem__(self, key, item):
        self.__dict__[key] = item

    def __getitem__(self, key):
        return self.__dict__[key]

    def add_item(self, *args, **kwargs):
        return


class MockGlobusAuth:
    def __init__(self):
        self.authorizer = None


class MockStrictRedis:
    def __init__(self, *args, **kwargs):
        # Use global MOCK_REDIS_CACHE so different RedisStores access the
        # same data
        self.data = MOCK_REDIS_CACHE

    def delete(self, key: str) -> None:
        if key in self.data:
            del self.data[key]

    def exists(self, key: str) -> bool:
        return key in self.data

    def get(self, key: str) -> Any:
        if key in self.data:
            return self.data[key]
        return None

    def set(self, key: str, value: Union[str, bytes, int, float]) -> None:
        if isinstance(value, (int, float)):
            value = str(value)
        if isinstance(value, str):
            value = value.encode()
        self.data[key] = value


def mock_third_party_libs() -> MonkeyPatch:
    """Get MonkeyPatch object for third party libs used by ProxyStore"""
    mpatch = MonkeyPatch()
    # Make new global MOCK_REDIS_CACHE
    global MOCK_REDIS_CACHE
    MOCK_REDIS_CACHE = {}
    mpatch.setattr(globus, "get_globus", MockGlobusAuth)
    mpatch.setattr(globus_sdk, "TransferClient", MockTransferClient)
    mpatch.setattr(globus_sdk, "DeleteData", MockDeleteData)
    mpatch.setattr(globus_sdk, "TransferData", MockTransferData)
    mpatch.setattr(socket, "gethostname", lambda: "localhost")
    mpatch.setattr(redis, "StrictRedis", MockStrictRedis)
    return mpatch
