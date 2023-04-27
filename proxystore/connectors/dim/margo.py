"""Margo RPC-based distributed in-memory connector implementation."""
from __future__ import annotations

import atexit
import multiprocessing
import logging
import sys
import time
import uuid
from enum import Enum
from os import getpid
from types import TracebackType
from typing import Any
from typing import NamedTuple
from typing import Sequence

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

try:
    import pymargo
    import pymargo.bulk as bulk
    from pymargo.bulk import Bulk
    from pymargo.core import Address
    from pymargo.core import Engine
    from pymargo.core import Handle
    from pymargo.core import MargoException
    from pymargo.core import RemoteFunction

    pymargo_import_error = None
except ImportError as e:  # pragma: no cover
    pymargo_import_error = e


import proxystore.utils as utils
from proxystore.connectors.dim.exceptions import ServerTimeoutError
from proxystore.connectors.dim.utils import get_ip_address
from proxystore.connectors.dim.models import DIMKey
from proxystore.connectors.dim.models import RPC
from proxystore.connectors.dim.models import RPCResponse
from proxystore.serialize import deserialize
from proxystore.serialize import serialize


# TODO: remove
server_process: multiprocessing.Process | None = None
client_pids: set[int] = set()
logger = logging.getLogger(__name__)
engine: Engine | None = None
_rpcs: dict[str, RemoteFunction]


class Protocol(Enum):
    """Available Mercury plugins and transports."""

    OFI_TCP = 'ofi+tcp'
    """libfabric tcp provider (TCP/IP)"""
    OFI_VERBS = 'ofi+verbs'
    """libfabric Verbs provider (InfiniBand or RoCE)"""
    OFI_GNI = 'ofi+gni'
    """libfabric GNI provider (Cray Aries)"""
    UCX_TCP = 'ucx+tcp'
    """UCX TCP/IP"""
    UCX_VERBS = 'ucx+verbs'
    """UCX Verbs"""
    SM_SHM = 'sm+shm'
    """Shared memory shm"""
    BMI_TCP = 'bmi+tcp'
    """BMI tcp module (TCP/IP)"""


class MargoConnector:
    """Margo RPC-based distributed in-memory connector.

    Note:
        The first instance of this connector created on a process will
        spawn a [`MargoServer`][proxystore.connectors.dim.margo.MargoServer]
        that will store data. Hence, this connector just acts as an interface
        to that server.

    Args:
        interface: The network interface to use.
        port: The desired port for the spawned server.
        protocol: The communication protocol to use.
    """

    host: str
    addr: str
    protocol: str
    engine: Engine
    _mochi_addr: Address
    _rpcs: dict[str, RemoteFunction]
    _pid: int

    # TODO : make host optional and try to get infiniband path automatically
    def __init__(
        self,
        interface: str,
        port: int,
        protocol: Protocol | str = Protocol.OFI_VERBS,
    ) -> None:
        global server_process
        global client_pids
        global engine
        global _rpcs

        # raise error if modules not properly loaded
        if pymargo_import_error is not None:  # pragma: no cover
            raise pymargo_import_error

        self.protocol = (
            protocol if isinstance(protocol, str) else protocol.value
        )

        self.interface = interface
        self.host = get_ip_address(interface)
        self.port = port

        self.addr = f'{self.protocol}://{self.host}:{port}'

        if engine is None:
            # start client
            engine = Engine(
                self.protocol,
                mode=pymargo.client,
                use_progress_thread=True,
            )

            _rpcs = {
                'set': engine.register('set'),
                'get': engine.register('get'),
                'exists': engine.register('exists'),
                'evict': engine.register('evict'),
            }

        self.engine = engine
        self._rpcs = _rpcs

        # TODO: do we keep?
        self._pid = getpid()
        client_pids.add(self._pid)

        self.server: multiprocessing.Process | None

        try:
            logger.info(
                f'Connecting to local server (address={self.addr})...',
            )
            wait_for_server(self.engine, self.protocol, self.host, self.port, self.timeout)
            logger.info(
                f'Connected to local server (address={self.addr})',
            )
        except AssertionError:
            logger.info(
                'Failed to connect to local server '
                f'(address={self.addr}, timeout={self.timeout})',
            )
            self.server = spawn_server(
                self.protocol,
                self.host,
                self.port,
                chunk_length=self.chunk_length,
                spawn_timeout=self.timeout,
            )
            logger.info(f'Spawned local server (address={self.addr})')
        else:
            self.server = None


    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def _send_rpcs(self, rpcs: Sequence[RPC]) -> list[RPCResponse]:
        """Send an RPC request to the server.

        Args:
            rpcs: List of RPCs to invoke on local server.

        Returns:
            List of RPC responses.

        Raises:
            Exception: Any exception returned by the local server.
        """
        responses = []

        for rpc in rpcs:
            addr = f'{self.protocol}://{rpc.key.peer_host}:{rpc.key.peer_port}'
            server_addr = self.engine.lookup(addr)
            logger.debug(
                f'Sent {rpc.operation.upper()} RPC (key={rpc.key})',
            )
            result = self._rpcs[rpc.operation].on(server_addr)(rpc.data, rpc.key.size, rpc.key)

            response = deserialize(result)
            logger.debug(
                f'Received {rpc.operation.upper()} RPC response '
                f'(key={response.key}, '
                f'exception={response.exception is not None})',
            )

            if response.exception is not None:
                raise response.exception

            assert rpc.operation == response.operation
            assert rpc.key == response.key

            responses.append(response)

        return responses

    # TODO: fix
    def close(self) -> None:
        """Close the connector and clean up.

        Warning:
            This will terminate the server is no clients are still connected.

        Warning:
            This method should only be called at the end of the program
            when the connector will no longer be used, for example once all
            proxies have been resolved.
        """
        global server_process
        global client_pids
        global engine

        client_pids.discard(self._pid)

        logger.info('Clean up requested')

        if len(client_pids) == 0 and server_process is not None:
            engine = None
            self._mochi_addr.shutdown()
            self.engine.finalize()
            server_process.join()
            server_process = None

    def config(self) -> dict[str, Any]:
        """Get the connector configuration.

        The configuration contains all the information needed to reconstruct
        the connector object.
        """
        return {
            'interface': self.interface,
            'port': self.port,
            'protocol': self.protocol,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> MargoConnector:
        """Create a new connector instance from a configuration.

        Args:
            config: Configuration returned by `#!python .config()`.
        """
        return cls(**config)

    def evict(self, key: MargoKey) -> None:
        """Evict the object associated with the key.

        Args:
            key: Key associated with object to evict.
        """
        logger.debug(f'Client issuing an evict request on key {key}')
        self.call_rpc_on(
            self.engine,
            key.peer,
            self._rpcs['evict'],
            '',
            key.margo_key,
            0,
        )

    def exists(self, key: MargoKey) -> bool:
        """Check if an object associated with the key exists.

        Args:
            key: Key potentially associated with stored object.

        Returns:
            If an object associated with the key exists.
        """
        logger.debug(f'Client issuing an exists request on key {key}')
        buff = bytearray(4)  # equivalent to malloc

        blk = self.engine.create_bulk(buff, bulk.write_only)

        self.call_rpc_on(
            self.engine,
            key.peer,
            self._rpcs['exists'],
            blk,
            key.margo_key,
            len(buff),
        )

        return bool(int(deserialize(bytes(buff))))

    def get(self, key: MargoKey) -> bytes | None:
        """Get the serialized object associated with the key.

        Args:
            key: Key associated with the object to retrieve.

        Returns:
            Serialized object or `None` if the object does not exist.
        """
        logger.debug(f'Client issuing get request on key {key}')

        buff = bytearray(key.obj_size)
        blk = self.engine.create_bulk(buff, bulk.read_write)
        s = self.call_rpc_on(
            self.engine,
            key.peer,
            self._rpcs['get'],
            blk,
            key.margo_key,
            key.obj_size,
        )

        if not s.success:
            logger.error(f'{s.error}')
            return None
        return bytes(buff)

    def get_batch(self, keys: Sequence[MargoKey]) -> list[bytes | None]:
        """Get a batch of serialized objects associated with the keys.

        Args:
            keys: Sequence of keys associated with objects to retrieve.

        Returns:
            List with same order as `keys` with the serialized objects or \
            `None` if the corresponding key does not have an associated object.
        """
        return [self.get(key) for key in keys]

    def put(self, obj: bytes) -> MargoKey:
        """Put a serialized object in the store.

        Args:
            obj: Serialized object to put in the store.

        Returns:
            Key which can be used to retrieve the object.
        """
        key = MargoKey(
            margo_key=str(uuid.uuid4()),
            obj_size=len(obj),
            peer=self.addr,
        )
        logger.debug(f'Client {self.addr} issuing set request on key {key}')
        blk = self.engine.create_bulk(obj, bulk.read_only)
        self.call_rpc_on(
            self.engine,
            self.addr,
            self._rpcs['set'],
            blk,
            key.margo_key,
            key.obj_size,
        )
        return key

    def put_batch(self, objs: Sequence[bytes]) -> list[MargoKey]:
        """Put a batch of serialized objects in the store.

        Args:
            objs: Sequence of serialized objects to put in the store.

        Returns:
            List of keys with the same order as `objs` which can be used to
            retrieve the objects.
        """
        return [self.put(obj) for obj in objs]


class MargoServer:
    """MargoServer implementation.

    Args:
        engine: The server engine created at the specified network address.
    """

    data: dict[str, bytes]
    engine: Engine

    def __init__(self, engine: Engine) -> None:
        self.data = {}

        self.engine = engine

        logger.debug('Server initialized')

    def set(
        self,
        handle: Handle,
        bulk_str: Bulk,
        bulk_size: int,
        key: str,
    ) -> None:
        """Obtain data from the client and store it in local dictionary.

        Args:
            handle: The client handle.
            bulk_str: The buffer containing the data to be shared.
            bulk_size: The size of the data being transferred.
            key: The data key.
        """
        logger.debug(f'Received set RPC for key {key}.')

        s = Status(True, None)

        local_buffer = bytearray(bulk_size)
        local_bulk = self.engine.create_bulk(local_buffer, bulk.write_only)
        self.engine.transfer(
            bulk.pull,
            handle.get_addr(),
            bulk_str,
            0,
            local_bulk,
            0,
            bulk_size,
        )
        self.data[key] = local_buffer

        handle.respond(serialize(s))

    def get(
        self,
        handle: Handle,
        bulk_str: Bulk,
        bulk_size: int,
        key: str,
    ) -> None:
        """Return data at a given key back to the client.

        Args:
            handle: The client handle.
            bulk_str: The buffer that will store shared data.
            bulk_size: The size of the data to be received.
            key: The data's key.
        """
        logger.debug(f'Received get RPC for key {key}.')

        s = Status(True, None)

        try:
            local_array = self.data[key]
            local_bulk = self.engine.create_bulk(local_array, bulk.read_only)
            self.engine.transfer(
                bulk.push,
                handle.get_addr(),
                bulk_str,
                0,
                local_bulk,
                0,
                bulk_size,
            )
        except KeyError as error:
            logger.error(f'key {error} not found.')
            s = Status(False, error)

        handle.respond(serialize(s))

    def evict(
        self,
        handle: Handle,
        bulk_str: str,
        bulk_size: int,
        key: str,
    ) -> None:
        """Remove key from local dictionary.

        Args:
            handle: The client handle.
            bulk_str: The buffer that will store shared data.
            bulk_size: The size of the data to be received.
            key: The data's key.
        """
        logger.debug(f'Received exists RPC for key {key}')

        self.data.pop(key, None)
        s = Status(True, None)

        handle.respond(serialize(s))

    def exists(
        self,
        handle: Handle,
        bulk_str: str,
        bulk_size: int,
        key: str,
    ) -> None:
        """Check if key exists within local dictionary.

        Args:
            handle: The client handle.
            bulk_str: The buffer that will store shared data.
            bulk_size: The size of the data to be received.
            key: The data's key.
        """
        logger.debug(f'Received exists RPC for key {key}')

        s = Status(True, None)

        # converting to int then string because length appears to be 7 for
        # True with pickle protocol 4 and cannot always guarantee that that
        # protocol will be selected
        local_array = serialize(str(int(key in self.data)))
        local_bulk = self.engine.create_bulk(local_array, bulk.read_only)
        size = len(local_array)
        self.engine.transfer(
            bulk.push,
            handle.get_addr(),
            bulk_str,
            0,
            local_bulk,
            0,
            size,
        )

        handle.respond(serialize(s))


def when_finalize() -> None:
    """Print a statement advising that engine finalization was triggered."""
    logger.info('Finalize was called. Cleaning up.')


def start_server(protocol: Protocol, host: str, port: int) -> None:
    """Launch the local Margo server (Peer) process.
    
    Args:
    
    """
    addr = f'{protocol}://{host}:{port}'
    server_engine = Engine(addr)
    server_engine.on_finalize(when_finalize)
    server_engine.enable_remote_shutdown()

    # create server
    receiver = MargoServer(server_engine)
    server_engine.register('get', receiver.get)
    server_engine.register('set', receiver.set)
    server_engine.register('exists', receiver.exists)
    server_engine.register('evict', receiver.evict)
    server_engine.wait_for_finalize()


def spawn_server(
    protocol: Protocol,
    host: str,
    port: int,
    *,
    spawn_timeout: float = 5.0,
    kill_timeout: float | None = 1.0,
) -> multiprocessing.Process:
    """Spawn a local server running in a separate process.

    Note:
        An `atexit` callback is registered which will terminate the spawned
        server process when the calling process exits.

    Args:
        protocol (enum): The margo network protocol to use for communication.
        host: The host of the server to ping.
        port: The port of the server to ping.
        spawn_timeout: Max time in seconds to wait for the server to start.
        kill_timeout: Max time in seconds to wait for the server to shutdown
            on exit.

    Returns:
        The process that the server is running in.
    """
    server_process = multiprocessing.Process(
        target=start_server,
        args=(protocol, host, port),
    )
    server_process.start()

    def _kill_on_exit() -> None:  # pragma: no cover
        server_process.terminate()
        server_process.join(timeout=kill_timeout)
        if server_process.is_alive():
            server_process.kill()
            server_process.join()
        logger.debug(
            'Server terminated on parent process exit '
            f'(pid={server_process.pid})',
        )

    atexit.register(_kill_on_exit)
    logger.debug('Registered server cleanup atexit callback')

    wait_for_server(protocol, host, port, timeout=spawn_timeout)
    logger.debug(
        f'Server started (host={host}, port={port}, pid={server_process.pid})',
    )

    return server_process


#TODO: removed the timeout here. Should verify that it'll work without it
def wait_for_server(engine: pymargo.core.Engine, protocol: Protocol, host: str, port: int, timeout: float = 0.1) -> None:
    """Wait until the server responds.

    Args:
        engine: The Margo client engine to use to connect to server.
        protocol (enum): The Margo network protocol to use for communication.
        host: The host of the server to ping.
        port: The port of the server to ping.

    Raises:
        ServerTimeoutError: If the server does not respond within the timeout.
    """
    start = time.time()
    addr = f"{protocol}://{host}:{port}"

    while time.time() - start < timeout:
        assert engine is not None
        try:
            engine.lookup(addr)
            break
        except MargoException:
            continue

    raise ServerTimeoutError(
        f'Failed to connect to server within timeout ({timeout} seconds).',
    )