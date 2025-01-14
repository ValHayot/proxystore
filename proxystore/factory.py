"""Factory implementations.

Factories are callable classes that wrap up the functionality needed
to resolve a proxy, where resolving is the process of retrieving the
object from wherever it is stored such that the proxy can act as the
object.
"""
from __future__ import annotations

from typing import Any
from typing import Callable
from typing import Generic
from typing import TypeVar

T = TypeVar('T')


class Factory(Generic[T]):
    """Abstract Factory Class.

    A factory is a callable object that when called, returns an object.
    The :any:`Proxy <proxystore.proxy.Proxy>` constructor takes an instance of
    a factory and calls the factory when the proxy does its just-in-time
    resolution.

    Note:
        All factory implementations must be subclasses of
        :class:`Factory <.Factory>`.

    Note:
        If a custom factory is not-pickleable,
        :func:`__getnewargs_ex__` may need to be implemented.
        Writing custom pickling functions is also beneifical to ensure that
        a pickled factory does not contain the object itself, just what is
        needed to resolve the object to keep the final pickled factory as
        small as possible.
    """

    def __init__(self) -> None:
        """Init Factory."""
        raise NotImplementedError

    def __call__(self) -> T:
        """Aliases :func:`resolve()`."""
        return self.resolve()

    def resolve(self) -> T:
        """Resolve and return object."""
        raise NotImplementedError


class SimpleFactory(Factory[T]):
    """Simple Factory that stores object as class attribute."""

    def __init__(self, obj: T) -> None:
        """Init Factory.

        Args:
            obj (object): object to produce when factory is called.
        """
        self._obj = obj

    def resolve(self) -> T:
        """Return object."""
        return self._obj


class LambdaFactory(Factory[T]):
    """Factory that takes any callable object."""

    def __init__(
        self,
        target: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Init LambdaFactory.

        Args:
            target (callable): callable object (function, class, lambda) to be
                invoked when the factory is resolved.
            args (tuple): argument tuple for target invocation (default: ()).
            kwargs (dict): dictionary of keyword arguments for target
                invocation (default: {}).
        """
        self._target = target
        self._args = args
        self._kwargs = kwargs

    def resolve(self) -> T:
        """Return target object."""
        return self._target(*self._args, **self._kwargs)
