from typing import List, Dict

from funtask.core import interface_and_types as interface
from funtask.core.interface_and_types import SchedulerNode, entities, RecordNotFoundException


class Scheduler(interface.Scheduler):
    def __init__(
            self,
            funtask_manager: interface.RPCFunTaskManager,
            repository: interface.Repository
    ):
        self.funtask_manager = funtask_manager
        self.repository = repository
        self.task_map: Dict[entities.CronTaskUUID, entities.CronTask] = {}

    async def assign_task(self, task_uuid: entities.TaskUUID):
        task = await self.repository.get_task_from_uuid(task_uuid)
        if task is None:
            raise RecordNotFoundException(f"record {task_uuid}")
        task_uuid_in_manager = await self.funtask_manager.dispatch_fun_task(
            worker_uuid=task.worker_uuid,
            func_task=task.func.func,
            dependencies=task.func.dependencies,
            change_status=task.result_as_state,
            timeout=task.timeout,
            argument=task.argument
        )
        await self.repository.update_task(task_uuid, {
            'status': entities.TaskStatus.QUEUED,
            'uuid_in_manager': task_uuid_in_manager
        })

    async def assign_cron_task(self, task_uuid: entities.CronTaskUUID):
        pass

    async def get_all_cron_task(self) -> List[entities.CronTaskUUID]:
        pass


class LeaderScheduler(interface.LeaderScheduler):
    async def scheduler_node_change(self, scheduler_nodes: List[SchedulerNode]):
        pass
