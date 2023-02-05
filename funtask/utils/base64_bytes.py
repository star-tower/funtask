import base64
import binascii
import contextlib
from typing import Any, Type

from pydantic import PydanticTypeError


class Base64Error(PydanticTypeError):
    msg_template = 'value is not valid base64'


@contextlib.contextmanager
def change_exception(from_exception: Type[Exception], to_exception: Type[Exception]):
    try:
        yield
    except from_exception as e:
        raise to_exception(e)


class Base64Bytes(bytes):
    def decode(self, encoding: str = ..., errors: str = ...) -> str:
        if encoding is ...:
            encoding = 'utf8'
        return base64.b64encode(self).decode(encoding)

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, value: Any) -> 'Base64Bytes':
        if isinstance(value, (bytes, str, bytearray, memoryview)):
            with change_exception(binascii.Error, Base64Error):
                base64.b64decode(value, validate=True)
            return Base64Bytes(value)
        if isinstance(value, int):
            raise Base64Error
        with change_exception(TypeError, Base64Error):
            encoded = base64.b64encode(bytes(value))
            return Base64Bytes(encoded)
