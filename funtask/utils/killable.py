import signal
import time
from dataclasses import dataclass
from typing import Any
from contextlib import contextmanager


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
