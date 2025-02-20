"""Proxy implementation and helpers."""
from __future__ import annotations

import sys
from typing import Any
from typing import Callable
from typing import Generic
from typing import TypeVar
from typing import Union

if sys.version_info >= (3, 8):  # pragma: >=3.8 cover
    from typing import SupportsIndex
else:  # pragma: <3.8 cover
    SupportsIndex = int

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import TypeAlias
else:  # pragma: <3.10 cover
    from typing_extensions import TypeAlias

from lazy_object_proxy import slots

import proxystore

T = TypeVar('T')
FactoryType: TypeAlias = Callable[[], T]


def _proxy_trampoline(factory: FactoryType[T]) -> Proxy[T]:
    """Trampoline for helping Proxy pickling.

    `slots.Proxy` defines a property for ``__modules__`` which confuses
    pickle when trying to locate the class in the module. The trampoline is
    a top-level function so pickle can correctly find it in this module.

    Args:
        factory (FactoryType): factory to pass to ``Proxy`` constructor.

    Returns:
        ``Proxy`` instance
    """
    return Proxy(factory)


class Proxy(slots.Proxy, Generic[T]):
    """Lazy Object Proxy.

    An extension of the Proxy from
    https://github.com/ionelmc/python-lazy-object-proxy with modified pickling
    behavior.

    An object proxy acts as a thin wrapper around a Python object, i.e.
    the proxy behaves identically to the underlying object. The proxy is
    initialized with a callable factory object. The factory returns the
    underlying object when called, i.e. 'resolves' the proxy. The does
    just-in-time resolution, i.e., the proxy
    does not call the factory until the first access to the proxy (hence, the
    lazy aspect of the proxy).

    The factory contains the mechanisms to appropriately resolve the object,
    e.g., which in the case for ProxyStore means requesting the correct
    object from the backend store.

    >>> x = np.array([1, 2, 3])
    >>> f = ps.factory.SimpleFactory(x)
    >>> p = ps.proxy.Proxy(f)
    >>> assert isinstance(p, np.ndarray)
    >>> assert np.array_equal(p, [1, 2, 3])

    Note:
        Due to ``Proxy`` modifying the ``__module__`` and ``__doc__``
        attributes, Sphinx cannot create autodocumentation for this
        class so any changes to the documentation here must be copied
        to ``docs/source/proxystore.proxy.rst``.

    Note:
        The `factory`, by default, is only ever called once during the
        lifetime of a proxy instance.

    Note:
        When a proxy instance is pickled, only the `factory` is pickled, not
        the wrapped object. Thus, proxy instances can be pickled and passed
        around cheaply, and once the proxy is unpickled and used, the `factory`
        will be called again to resolve the object.

    Warning:
        Python bindings to other languages (e.g., C, C++) may throw type
        errors when receiving a :class:`~proxystore.proxy.Proxy`.
        Casting the proxy or extracting the target object may be needed.

        .. code-block:: python

           >>> import io
           >>> from proxystore.proxy import Proxy
           >>> s = 'mystring'
           >>> p = Proxy(lambda: s)
           >>> io.StringIO(p)
           Traceback (most recent call last):
             File "<stdin>", line 1, in <module>
           TypeError: initial_value must be str or None, not Proxy
           >>> io.StringIO(str(p))  # succeeds
    """

    def __init__(self, factory: FactoryType[T]) -> None:
        """Init Proxy.

        Args:
            factory (Factory): callable object that returns the
                underlying object when called.

        Raises:
            TypeError:
                if `factory` is not callable.
        """
        if not callable(factory):
            raise TypeError('factory must be callable')
        super().__init__(factory)

    def __reduce__(
        self,
    ) -> tuple[Callable[[FactoryType[T]], Proxy[T]], tuple[FactoryType[T]]]:
        """Use trampoline function for pickling.

        Override `Proxy.__reduce__` so that we only pickle the Factory
        and not the object itself to reduce size of the pickle.
        """
        return _proxy_trampoline, (
            object.__getattribute__(self, '__factory__'),
        )

    def __reduce_ex__(
        self,
        protocol: SupportsIndex,
    ) -> tuple[Callable[[FactoryType[T]], Proxy[T]], tuple[FactoryType[T]]]:
        """See `__reduce__`."""
        return self.__reduce__()


ProxyType: TypeAlias = Union[Proxy[T], T]


def extract(proxy: proxystore.proxy.Proxy[T]) -> T:
    """Return object wrapped by proxy.

    If the proxy has not been resolved yet, this will force
    the proxy to be resolved prior.

    Args:
        proxy (Proxy): proxy instance to extract from.

    Returns:
        object wrapped by proxy.
    """
    return proxy.__wrapped__


def is_resolved(proxy: proxystore.proxy.Proxy[T]) -> bool:
    """Check if a proxy is resolved.

    Args:
        proxy (Proxy): proxy instance to check.

    Returns:
        `True` if `proxy` is resolved (i.e., the `factory` has been called) and
        `False` otherwise.
    """
    return proxy.__resolved__


def resolve(proxy: proxystore.proxy.Proxy[T]) -> None:
    """Force a proxy to resolve itself.

    Args:
        proxy (Proxy): proxy instance to force resolve.
    """
    proxy.__wrapped__


class ProxyLocker(Generic[T]):
    """Proxy locker that prevents resolution of wrapped proxies.

    The :class:`~proxystore.proxy.ProxyLocker` unintended access to a wrapped
    proxy to ensure a proxy is not resolved. The wrapped proxy can
    be retrieved with :func:`~proxystore.proxy.ProxyLocker.unlock`.
    """

    def __init__(self, proxy: Proxy[T]) -> None:
        """Init ProxyLocker.

        Args:
            proxy (Proxy[T]): proxy to lock.
        """
        self._proxy = proxy

    def __getattribute__(self, attr: str) -> Any:
        """Override to raise an error if the proxy is accessed."""
        if attr == '_proxy':
            raise AttributeError('Cannot access proxy attribute of a Locker')
        return super().__getattribute__(attr)

    def unlock(self) -> Proxy[T]:
        """Retrieve the locked proxy.

        Returns:
            proxy object.
        """
        return super().__getattribute__('_proxy')
