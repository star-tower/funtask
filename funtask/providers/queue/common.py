from funtask.core.interface_and_types import BreakRef


class NeverBreak(BreakRef):
    def if_break_now(self) -> bool:
        return False
