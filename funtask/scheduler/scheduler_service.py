from grpclib.server import Server

from dependency_injector.wiring import inject, Provide
from loguru import logger

from funtask.core.entities import SchedulerNode
from funtask.core.scheduler import Scheduler
from funtask.generated import Empty
from funtask.generated.scheduler import SchedulerBase, AssignTaskRequest, GetTaskListResponse, RemoveTaskRequest


class SchedulerService(SchedulerBase):
    def __init__(self, scheduler: Scheduler):
        self.scheduler = scheduler

    async def assign_task(self, assign_task_request: AssignTaskRequest) -> Empty:
        if assign_task_request.task_uuid:
            await self.scheduler.worker_scheduler.assign_task(assign_task_request.task_uuid)
        if assign_task_request.cron_task_uuid:
            await self.scheduler.worker_scheduler.assign_cron_task(assign_task_request.cron_task_uuid)
        return Empty()

    async def get_task_list(self, empty: Empty) -> GetTaskListResponse:
        cron_tasks = await self.scheduler.worker_scheduler.get_all_cron_task()
        return GetTaskListResponse(cron_tasks)

    async def remove_task(self, remove_task_request: RemoveTaskRequest) -> Empty:
        await self.scheduler.worker_scheduler.remove_cron_task(remove_task_request.cron_task_uuid)
        return Empty()


class SchedulerServiceRunner:
    @inject
    def __init__(
            self,
            scheduler: Scheduler = Provide['scheduler.scheduler'],
            scheduler_node: SchedulerNode = Provide['scheduler.node']
    ):
        self.scheduler = scheduler
        self.scheduler_node = scheduler_node

    async def run(self):
        server = Server([SchedulerService(
            self.scheduler
        )])
        logger.opt(colors=True).info(
            "starting grpc service <cyan>{address}:{port}</cyan>",
            address=self.scheduler_node.host,
            port=self.scheduler_node.port
        )
        await server.start(self.scheduler_node.host, self.scheduler_node.port)
        logger.info("service start successful")
        await self.scheduler.run()
        await server.wait_closed()
