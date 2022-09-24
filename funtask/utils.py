import importlib
import signal
import time
from dataclasses import dataclass
from enum import Enum
from typing import List, Dict, Any
from contextlib import contextmanager


class AutoName(Enum):
    @staticmethod
    def _generate_next_value_(name: str, start: int, count: int, last_values: List) -> str:
        return name


class ShadowGlobal:
    """
    magic happens here, any change via `shadow_global` can be recovered
    """

    def __init__(self):
        self.origin = {}
        self.extras = []

    def shadow_global(self, new_global: Dict[str, Any], globals_: Dict = None):
        globals_ = globals_ or globals()
        # pop extra variables
        for extra in self.extras:
            globals_.pop(extra, None)
        self.extras = []
        # recover origin variables
        globals_.update(self.origin)
        self.origin = {}
        # restore origin and extras
        for var_name in new_global:
            if var_name in globals_:
                self.origin[var_name] = globals_[var_name]
            else:
                self.extras.append(var_name)
        globals_.update(new_global)


class ImportMixInGlobal:
    def __init__(self):
        self.sg = ShadowGlobal()
        self.imported = set()

    def import_module_globally(self, path: str | List[str], globals_: Dict = None):
        if isinstance(path, List):
            module_all = self.imports_as_dict(path)
        else:
            module_all = self.import_as_dict(path)
        self.sg.shadow_global(module_all, globals_)

    @staticmethod
    def import_as_dict(path: str) -> Dict[str, Any]:
        mdl = importlib.import_module(path)
        if "__all__" in mdl.__dict__:
            items = mdl.__dict__["__all__"]
        else:
            items = [item for item in mdl.__dict__ if not item.startswith("_")]
        return {
            item: getattr(mdl, item) for item in items
        }

    @staticmethod
    def imports_as_dict(path: List[str]) -> Dict[str, Any]:
        module_all = {}
        for p in path:
            module_all.update(ImportMixInGlobal.import_as_dict(p))
        return module_all


@dataclass
class Ref:
    value: Any


class FuncStopException(Exception):
    ...


class KillException(Exception):
    ...


@contextmanager
def killable(timeout: int = None, mute: bool = True):
    assert timeout is None or timeout > 0, Exception("timeout should > 0")
    sig_ref = Ref(False)

    def kill():
        sig_ref.value = True

    def sigalrm_handler(signum, frame):
        if timeout and time.time() - start_time > timeout:
            raise FuncStopException

        if not sig_ref.value:
            signal.alarm(1)
        else:
            raise FuncStopException

    ori_signal = signal.getsignal(signal.SIGALRM)

    signal.signal(signal.SIGALRM, sigalrm_handler)
    signal.alarm(1)

    try:
        start_time = time.time()
        yield kill
    except FuncStopException:
        if not mute:
            raise KillException("killed.")
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, ori_signal)
