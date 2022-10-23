from funtask.core.interface_and_types import Scheduler as SchedulerAbs, FunTaskManager, \
    LeaderScheduler as LeaderSchedulerAbs


class Scheduler(SchedulerAbs):
    def __init__(
            self,
            funtask_manager: FunTaskManager
    ):
        self.funtask_manager = funtask_manager


class LeaderScheduler(LeaderSchedulerAbs):
    ...
