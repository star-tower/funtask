from funtask.core.funtask_types import BreakRef


class NeverBreak(BreakRef):
    def if_break_now(self) -> bool:
        return False
