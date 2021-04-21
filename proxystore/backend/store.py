"""Backend Key-Value Store Implementations"""
import os
import time

from typing import Any, Dict, Optional

try:
    import redis
except ImportError as e:  # pragma: no cover
    # We do not want to raise this ImportError if the user never
    # uses the RedisStore so we delay raising the error until the
    # constructor of RedisStore
    redis = e

from proxystore.backend.serialize import serialize, deserialize
from proxystore.backend.cache import LRUCache

PROXYSTORE_CACHE_SIZE_ENV = 'PROXYSTORE_CACHE_SIZE'


class BaseStore:
    """Backend Store Abstract Class"""

    def evict(self, key: str) -> None:
        """Evict value associated with key from store"""
        raise NotImplementedError

    def exists(self, key: str) -> bool:
        """Check if key exists in store"""
        raise NotImplementedError

    def get(self, key: str, strict: bool = False) -> Optional[object]:
        """Get value corresponding to key in store

        Args:
            key (str): key corresponding to value in the store
            strict (bool): if True, guarentee returned value is the most
                recent value associated with key

        Returns:
            value associated with key or None if key does not exist
        """
        raise NotImplementedError

    def is_cached(self, key: str, strict: bool = False) -> bool:
        """Check if value associated with key is cached

        Args:
            key (str): key to check if cached
            strict (bool): if True, guarentee that cached value is the most
                recent value associated with key

        Returns:
            boolean
        """
        raise NotImplementedError

    def set(self, key: str, obj: Any) -> None:
        """Set key-value pair in store"""
        raise NotImplementedError


class LocalStore(BaseStore):
    """Local Memory Store"""

    def __init__(self) -> None:
        """Init LocalStore"""
        self.store = {}

    def evict(self, key: str) -> None:
        """Evict value associated with key from store"""
        if key in self.store:
            del self.store[key]

    def exists(self, key: str) -> bool:
        """Check if key exists in store"""
        return key in self.store

    def get(self, key: str, strict: bool = False) -> Optional[object]:
        """Get value corresponding to key in store

        Args:
            key (str): key corresponding to value in the store
            strict (bool): if `True`, guarentee returned value is the most
                recent value associated with `key`

        Returns:
            value associated with `key` or None if `key` does not exist
        """
        if key in self.store:
            return self.store[key]
        return None

    def is_cached(self, key: str, strict: bool = False) -> bool:
        """Check if value associated with key is cached

        Args:
            key (str): key to check if cached
            strict (bool): if True, guarentee that cached value is the most
                recent value associated with key

        Returns:
            boolean
        """
        return key in self.store

    def set(self, key: str, obj: Any) -> None:
        """Set key-value pair in store"""
        self.store[key] = obj


class CachedStore(BaseStore):
    """Base class for backends with caching support

    Classes extending `BaseStore` must implement `evict()`, `exists()`
    `get_str()` and `set_str()`. The BaseStore handles the cache.
    The cache stores key: (timestamp, obj) pairs.
    """

    def __init__(self, cache_size: int = 16) -> None:
        """Init CachedStore

        Args:
            cache_size (int): number of objects cache can hold
        """
        if cache_size < 0:
            raise ValueError('Cache size cannot be negative')
        env_cache_size = os.environ.get(PROXYSTORE_CACHE_SIZE_ENV, None)
        if env_cache_size is not None:
            cache_size = int(env_cache_size)
        self._cache = LRUCache(cache_size) if cache_size > 0 else None

    def evict(self, key: str) -> None:
        """Evict value associated with key from store"""
        raise NotImplementedError

    def exists(self, key: str) -> bool:
        """Check if key exists in store"""
        raise NotImplementedError

    def get_str(self, key: str) -> Optional[str]:
        """Get string associated with key from store"""
        raise NotImplementedError

    def set_str(self, key: str, data: str) -> None:
        """Set key-string pair in store"""
        raise NotImplementedError

    def get(self, key: str, strict: bool = False) -> Optional[object]:
        """Get value corresponding to key in store

        Args:
            key (str): key corresponding to value in the store
            strict (bool): if `True`, guarentee returned value is the most
                recent value associated with `key`

        Returns:
            value associated with `key` or None if `key` does not exist
        """
        if self.is_cached(key, strict):
            return self._cache.get(key)[1]

        value = self.get_str(key)
        if value is not None:
            timestamp = float(self.get_str(key + '_timestamp'))
            obj = deserialize(value)
            if self._cache is not None:
                self._cache.set(key, (timestamp, obj))
            return obj

        return None

    def is_cached(self, key: str, strict: bool = False) -> bool:
        """Check if value associated with key is cached

        Args:
            key (str): key to check if cached
            strict (bool): if True, guarentee that cached value is the most
                recent value associated with key

        Returns:
            boolean
        """
        if self._cache is None:
            return False

        if self._cache.exists(key):
            if strict:
                store_timestamp = float(self.get_str(key + '_timestamp'))
                cache_timestamp = self._cache.get(key)[0]
                return cache_timestamp >= store_timestamp
            return True
        return False

    def set(self, key: str, obj: Any) -> None:
        """Set key-value pair in store"""
        obj = serialize(obj)

        self.set_str(key, obj)
        self.set_str(key + '_timestamp', str(time.time()))


class RedisStore(CachedStore):
    """Redis backend class"""

    def __init__(
        self, hostname: str, port: int, **kwargs: Dict[str, Any]
    ) -> None:
        """Init RedisStore

        Args:
            hostname (str): Redis server hostname
            port (int): Redis server port
        """
        if isinstance(redis, ImportError):  # pragma: no cover
            raise ImportError(
                'The redis-py package must be installed to use the '
                'RedisStore backend'
            )

        self.hostname = hostname
        self.port = port
        self.redis_client = redis.StrictRedis(
            host=hostname, port=port, decode_responses=True
        )
        super(RedisStore, self).__init__(**kwargs)

    def evict(self, key: str) -> None:
        """Evict value corresponding to `key` from store"""
        self.redis_client.delete(key)

    def exists(self, key: str) -> bool:
        """Check if key exists in store"""
        return self.redis_client.exists(key)

    def get_str(self, key: str) -> Optional[str]:
        """Get string associated with key from store"""
        return self.redis_client.get(key)

    def set_str(self, key: str, data: str) -> None:
        """Set key-string pair in store"""
        self.redis_client.set(key, data)