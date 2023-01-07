import inspect
from functools import wraps
from typing import Callable
from asyncio import iscoroutinefunction


class SelfNotSetException(Exception):
    ...


class SelfPointer:
    def __init__(self):
        self.self = None
        self.be_set = False

    def set_self(self, self_):
        self.self = self_
        self.be_set = True

    def get_self(self):
        if not self.be_set:
            raise SelfNotSetException('self is not be set before get_self')
        return self.self


def self_wrapper(self_pointer: SelfPointer) -> Callable:
    def decorator(f: Callable) -> Callable:
        if iscoroutinefunction(f):
            @wraps(f)
            async def f_helper(*args, **kwargs):
                return await f(self_pointer.get_self(), *args, **kwargs)
        else:
            @wraps(f)
            def f_helper(*args, **kwargs):
                return f(self_pointer.get_self(), *args, **kwargs)

        ori_sign = inspect.signature(f)
        res_sign = ori_sign.replace(parameters=tuple(ori_sign.parameters.values())[1:])

        f_helper.__signature__ = res_sign

        return f_helper

    return decorator
