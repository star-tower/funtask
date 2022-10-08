from typing import Tuple, Any, Dict
import dill

from funtask import WorkerStatus, TaskStatus
from funtask.generated.manager import Args, StatusReportTaskStatus, StatusReportWorkerStatus


def load_args(args: Args) -> Tuple[Tuple[Any, ...], Dict[str, Any]]:
    return dill.loads(args.serialized_args), dill.loads(args.serialized_kwargs)


def dump_args(*args, **kwargs) -> Args:
    return Args(
        serialized_args=dill.dumps(args),
        serialized_kwargs=dill.dumps(kwargs)
    )


def core_status2rpc_status(core_status: WorkerStatus | TaskStatus)\
        -> Dict[str, StatusReportTaskStatus | StatusReportWorkerStatus]:
    if isinstance(core_status, WorkerStatus):
        return {'task_status': StatusReportTaskStatus.from_string(core_status.value)}  # type: ignore
    else:
        return {'worker_status': StatusReportWorkerStatus.from_string(core_status.value)}  # type: ignore
