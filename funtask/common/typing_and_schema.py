from typing import Callable, Dict, Set
from pydantic import create_model
from enum import Enum
import inspect

from pydantic.fields import Undefined, FieldInfo


class ConvertFlag(Enum):
    IGNORE = 'IGNORE'


def function_parameters2schema(
        func: Callable,
        flags: Dict[type, ConvertFlag] | None = None,
        model_name: str | None = None
) -> Dict:
    """
    convert function parameters to json schema
    :param func: function for extract schema
    :param flags: flag for specific type
    :param model_name: name of generated model, default is function name
    :return: json schema as dict
    >>> from dataclasses import dataclass
    >>> from datetime import datetime
    >>> from enum import Enum
    >>> class E(Enum):
    ...     a = 'a'
    >>> @dataclass
    ... class B:
    ...     d: int
    ...     f: datetime
    >>> @dataclass
    ... class A:
    ...     b: int
    ...     c: str
    ...     g: B
    ...     e: E
    >>> def test_func(i: str, l: int, a: A) -> str:
    ...     ...
    >>> res = function_parameters2schema(test_func)
    >>> import json
    >>> json.dumps(res, sort_keys=True)
    '{"definitions": {"A": {"properties": {"b": {"title": "B", "type": "integer"}, "c": {"title": "C", "type": "string"}, "e": {"$ref": "#/definitions/E"}, "g": {"$ref": "#/definitions/B"}}, "required": ["b", "c", "g", "e"], "title": "A", "type": "object"}, "B": {"properties": {"d": {"title": "D", "type": "integer"}, "f": {"format": "date-time", "title": "F", "type": "string"}}, "required": ["d", "f"], "title": "B", "type": "object"}, "E": {"description": "An enumeration.", "enum": ["a"], "title": "E"}}, "properties": {"a": {"$ref": "#/definitions/A"}, "i": {"title": "I", "type": "string"}, "l": {"title": "L", "type": "integer"}}, "title": "test_func", "type": "object"}'
    """
    if model_name is None:
        model_name = func.__name__
    if flags is None:
        flags = {}
    sig = inspect.signature(func)
    columns = {}
    for name, parameter_sig in sig.parameters.items():
        if parameter_sig.annotation in flags:
            match flags[parameter_sig.annotation]:
                case ConvertFlag.IGNORE:
                    continue
        columns[name] = (parameter_sig.annotation, FieldInfo(
            default=None if parameter_sig.default is inspect.Parameter.empty else Undefined
        ))

    return create_model(model_name, **columns).schema()
