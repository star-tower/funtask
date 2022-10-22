from funtask.core.funtask_types.task_worker_manager import FunTaskManager
from funtask.core.funtask_types.scheduler import Scheduler as SchedulerAbs, LeaderScheduler as LeaderSchedulerAbs


class Scheduler(SchedulerAbs):
    def __init__(
            self,
            funtask_manager: FunTaskManager
    ):
        self.funtask_manager = funtask_manager


class LeaderScheduler(LeaderSchedulerAbs):
    ...
