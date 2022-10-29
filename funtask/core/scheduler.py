import uuid
from typing import List, Dict, Any, cast

from funtask.core import interface_and_types as interface
from funtask.core.interface_and_types import SchedulerNode, entities, RecordNotFoundException
from dataclasses import asdict


def _task_with_point2name(cron_task: entities.CronTask, time_point: entities.TimePoint) -> str:
    return f"{cron_task.uuid}/{time_point}"


class Scheduler(interface.Scheduler):
    def __init__(
            self,
            funtask_manager: interface.RPCFunTaskManager,
            repository: interface.Repository,
            cron: interface.Cron,
            argument_queue_factory: interface.QueueFactory
    ):
        self.funtask_manager = funtask_manager
        self.repository = repository
        self.task_map: Dict[entities.CronTaskUUID, entities.CronTask] = {}
        self.cron = cron
        self.argument_queue_factory = argument_queue_factory

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

    async def _resolve_task_queue_strategy(
            self,
            task_queue_strategy: entities.QueueStrategy,
            info_dict: Dict[str, Any],
            max_depth=10,
            depth=0
    ) -> entities.QueueStrategy:
        """
        resolve queue strategy to no udf strategy
        :param task_queue_strategy: strategy on task queue full
        :param info_dict: arguments of UDF
        :return: strategy with no udf
        """
        if depth > max_depth:
            raise RecursionError(f"max depth of resolve queue strategy {max_depth}")
        if task_queue_strategy != entities.QueueFullStrategy.UDF:
            return task_queue_strategy
        assert task_queue_strategy.udf is not None, ValueError("UDF strategy must not None")
        new_strategy = await task_queue_strategy.udf(info_dict)
        return await self._resolve_task_queue_strategy(new_strategy, info_dict, depth=depth + 1)

    async def _resolve_argument_strategy(
            self,
            argument_strategy: entities.ArgumentStrategy,
            info_dict: Dict[str, Any],
            max_depth=10,
            depth=0
    ) -> entities.ArgumentStrategy:
        """
        resolve argument strategy to no udf strategy
        :param argument_strategy: argument generate strategy
        :param info_dict: arguments of UDF
        :return: strategy with no udf
        """
        if depth > max_depth:
            raise RecursionError(f"max depth of resolve queue strategy {max_depth}")
        if argument_strategy != entities.ArgumentGenerateStrategy.UDF:
            return argument_strategy
        assert argument_strategy.udf is not None, ValueError("UDF strategy must not None")
        new_strategy = await argument_strategy.udf(info_dict)
        return await self._resolve_argument_strategy(new_strategy, info_dict, depth=depth + 1)

    async def _create_cron_sub_task(self, cron_task: entities.CronTask):
        new_task_uuid = cast(entities.TaskUUID, str(uuid.uuid4()))
        argument_strategy = await self._resolve_argument_strategy(
            cron_task.argument_generate_strategy,
            asdict(cron_task)
        )
        match argument_strategy.strategy:
            case entities.ArgumentGenerateStrategy.DROP:
                return
            case entities.ArgumentGenerateStrategy.SKIP:
                await self.repository.add_task(entities.Task(
                    uuid=new_task_uuid,
                    parent_task=cron_task.uuid,
                    uuid_in_manager=None,
                    status=entities.TaskStatus.SKIP,
                    worker_uuid=None,
                    func=cron_task.func,
                    argument=None,
                    result_as_state=cron_task.result_as_state,
                    timeout=cron_task.timeout,
                    description=cron_task.description
                ))
            case entities.ArgumentGenerateStrategy.STATIC:
                await self.repository.add_task(entities.Task(
                    uuid=new_task_uuid,
                    parent_task=cron_task.uuid,
                    uuid_in_manager=None,
                    status=entities.TaskStatus.SCHEDULED,
                    worker_uuid=None,
                    func=cron_task.func,
                    argument=cron_task.argument_generate_strategy.static_value,
                    result_as_state=cron_task.result_as_state,
                    timeout=cron_task.timeout,
                    description=cron_task.description
                ))
            case entities.ArgumentGenerateStrategy.FROM_QUEUE_END_DROP | \
                 entities.ArgumentGenerateStrategy.FROM_QUEUE_END_SKIP | \
                 entities.ArgumentGenerateStrategy.FROM_QUEUE_END_REPEAT_LATEST:
                assert argument_strategy.argument_queue is not None, ValueError(
                    f"must assign argument queue if strategy is {argument_strategy.strategy}"
                )
                argument_queue = self.argument_queue_factory(
                    argument_strategy.argument_queue.name
                )
                qsize = await argument_queue.qsize()
                if qsize != 0:
                    argument = await argument_queue.get(0)
                    await self.repository.add_task(entities.Task(
                        uuid=new_task_uuid,
                        parent_task=cron_task.uuid,
                        uuid_in_manager=None,
                        status=entities.TaskStatus.SCHEDULED,
                        worker_uuid=None,
                        func=cron_task.func,
                        argument=argument,
                        result_as_state=cron_task.result_as_state,
                        timeout=cron_task.timeout,
                        description=cron_task.description
                    ))
                else:
                    match argument_strategy.strategy:
                        case entities.ArgumentGenerateStrategy.FROM_QUEUE_END_DROP:
                            return
                        case entities.ArgumentGenerateStrategy.FROM_QUEUE_END_SKIP:
                            await self.repository.add_task(entities.Task(
                                uuid=new_task_uuid,
                                parent_task=cron_task.uuid,
                                uuid_in_manager=None,
                                status=entities.TaskStatus.SKIP,
                                worker_uuid=None,
                                func=cron_task.func,
                                argument=None,
                                result_as_state=cron_task.result_as_state,
                                timeout=cron_task.timeout,
                                description=cron_task.description
                            ))
                        case entities.ArgumentGenerateStrategy.FROM_QUEUE_END_REPEAT_LATEST:
                            try:
                                argument = await argument_queue.get_front()
                            except interface.EmptyQueueException:
                                await self.repository.add_task(entities.Task(
                                    uuid=new_task_uuid,
                                    parent_task=cron_task.uuid,
                                    uuid_in_manager=None,
                                    status=entities.TaskStatus.ERROR,
                                    worker_uuid=None,
                                    func=cron_task.func,
                                    argument=None,
                                    result_as_state=cron_task.result_as_state,
                                    timeout=cron_task.timeout,
                                    description=cron_task.description,
                                    result=f"empty argument queue on {cron_task.argument_generate_strategy.strategy} mod"
                                ))
                                return
                            await self.repository.add_task(entities.Task(
                                uuid=new_task_uuid,
                                parent_task=cron_task.uuid,
                                uuid_in_manager=None,
                                status=entities.TaskStatus.SCHEDULED,
                                worker_uuid=None,
                                func=cron_task.func,
                                argument=argument,
                                result_as_state=cron_task.result_as_state,
                                timeout=cron_task.timeout,
                                description=cron_task.description
                            ))

        await self.assign_task(new_task_uuid)

    async def assign_cron_task(self, task_uuid: entities.CronTaskUUID):
        cron_task = await self.repository.get_cron_task_from_uuid(task_uuid)
        for time_point in cron_task.timepoints:
            match time_point.unit:
                case entities.TimeUnit.DAY:
                    await self.cron.every_n_days(
                        _task_with_point2name(cron_task, time_point),
                        time_point.n,
                        self._create_cron_sub_task,
                        time_point.at,
                        cron_task
                    )

    async def get_all_cron_task(self) -> List[entities.CronTaskUUID]:
        pass


class LeaderScheduler(interface.LeaderScheduler):
    async def scheduler_node_change(self, scheduler_nodes: List[SchedulerNode]):
        pass
