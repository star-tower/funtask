from typing import Tuple, Any, Dict
import dill

from funtask import WorkerStatus, TaskStatus
from funtask.generated import Args, StatusReportTaskStatus, StatusReportWorkerStatus


def load_args(args: Args) -> Tuple[Tuple[Any, ...], Dict[str, Any]]:
    res_args = []
    res_kwargs = {}

    for rpc_arg in args.serialized_args:
        res_args.append(dill.loads(rpc_arg))

    for rpc_kwargs in args.serialized_kwargs:
        k, v = dill.loads(rpc_kwargs)
        res_args[k] = v

    return tuple(res_args), res_kwargs


def dump_args(*args, **kwargs) -> Args:
    return Args(
        serialized_args=dill.dumps(args),
        serialized_kwargs=dill.dumps(kwargs)
    )


def core_status2rpc_status(core_status: WorkerStatus | TaskStatus | None)\
        -> Dict[str, StatusReportTaskStatus | StatusReportWorkerStatus | None]:
    if core_status is None:
        return {
            'worker_status': StatusReportWorkerStatus.HEARTBEAT
        }
    if isinstance(core_status, TaskStatus):
        return {
            'task_status': StatusReportTaskStatus.from_string(core_status.name)
        }  # type: ignore
    else:
        return {
            'worker_status': StatusReportWorkerStatus.from_string(core_status.name)
        }  # type: ignore
