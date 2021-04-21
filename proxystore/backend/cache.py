"""Simple Cache Implementation"""
from typing import Any


class LRUCache:
    """Simple LRU Cache"""

    def __init__(self, maxsize: int = 16) -> None:
        """Init LRUCache

        Args:
            maxsize (int): maximum number of value to cache
        """
        if maxsize <= 0:
            raise ValueError('Cache size must by > 0')
        self.maxsize = maxsize
        self.data = {}
        self.lru = []

        # Count hits/misses
        self.hits = 0
        self.misses = 0

    def exists(self, key: Any) -> bool:
        """Check if key is cached"""
        return key in self.data

    def get(self, key: Any, default: Any = None) -> Any:
        """Get value for key if it exists else returns `default`"""
        if self.exists(key):
            # Move to front b/c most recently used
            self.hits += 1
            self.lru.remove(key)
            self.lru.insert(0, key)
            return self.data[key]
        else:
            self.misses += 1
            return default

    def set(self, key: Any, value: Any) -> None:
        """Set key to value"""
        if len(self.data) >= self.maxsize:
            lru_key = self.lru.pop()
            del self.data[lru_key]
        self.lru.insert(0, key)
        self.data[key] = value