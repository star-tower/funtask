from funtask.core.funtask_types.task_worker_manager import BreakRef


class NeverBreak(BreakRef):
    def if_break_now(self) -> bool:
        return False
