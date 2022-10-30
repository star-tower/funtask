import random
import uuid
from typing import List, Dict, Any, cast

from funtask.core import interface_and_types as interface
from funtask.core.interface_and_types import SchedulerNode, entities, RecordNotFoundException
from dataclasses import asdict


def _task_with_point2name(cron_task: entities.CronTask, time_point: entities.TimePoint) -> str:
    return f"{cron_task.uuid}/{time_point}"


def _task_from_name(name: str) -> entities.CronTaskUUID:
    return cast(name.split('/')[0], entities.CronTaskUUID)


def _filter_task_uuid_from_names(names: List[str], task_uuid: entities.CronTaskUUID) -> List[str]:
    return [name for name in names if name.startswith(task_uuid)]


class Scheduler(interface.Scheduler):
    def __init__(
            self,
            funtask_manager: interface.RPCFunTaskManager,
            repository: interface.Repository,
            cron: interface.Cron,
            argument_queue_factory: interface.QueueFactory,
            lock: interface.DistributeLock
    ):
        self.funtask_manager = funtask_manager
        self.repository = repository
        self.task_map: Dict[entities.CronTaskUUID, entities.CronTask] = {}
        self.cron = cron
        self.argument_queue_factory = argument_queue_factory
        self.lock = lock

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

    async def remove_cron_task(self, task_uuid: entities.CronTaskUUID) -> bool:
        all_cron_tasks = await self.cron.get_all()
        target_cron_tasks = _filter_task_uuid_from_names(all_cron_tasks, task_uuid)
        [await self.cron.cancel(target_cron_task) for target_cron_task in target_cron_tasks]
        return True

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
        if task_queue_strategy.full_strategy != entities.QueueFullStrategy.UDF:
            return task_queue_strategy
        assert task_queue_strategy.udf is not None, ValueError("task UDF strategy must not None")
        if task_queue_strategy.udf_extra:
            info_dict.update(task_queue_strategy.udf_extra)
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
        assert argument_strategy.udf is not None, ValueError("argument UDF strategy must not None")
        if argument_strategy.udf_extra:
            info_dict.update(argument_strategy.udf_extra)
        new_strategy = await argument_strategy.udf(info_dict)
        return await self._resolve_argument_strategy(new_strategy, info_dict, depth=depth + 1)

    async def _resolve_worker_choose_strategy(
            self,
            worker_choose_strategy: entities.WorkerStrategy,
            info_dict: Dict[str, Any],
            max_depth=10,
            depth=0
    ) -> entities.WorkerStrategy:
        """
        resolve argument strategy to no udf strategy
        :param worker_choose_strategy: worker choose strategy
        :param info_dict: arguments of UDF
        :return: strategy with no udf
        """
        if depth > max_depth:
            raise RecursionError(f"max depth of resolve queue strategy {max_depth}")
        if worker_choose_strategy.strategy != entities.WorkerChooseStrategy.UDF:
            return worker_choose_strategy
        assert worker_choose_strategy.udf is not None, ValueError("worker choose UDF strategy must not None")
        if worker_choose_strategy.udf_extra:
            info_dict.update(worker_choose_strategy.udf_extra)
        new_strategy = await worker_choose_strategy.udf(info_dict)
        return await self._resolve_worker_choose_strategy(new_strategy, info_dict, depth=depth + 1)

    async def _choose_worker_from_worker_choose_strategy(
            self,
            cron_task: entities.CronTask,
            worker_choose_strategy: entities.WorkerStrategy
    ) -> entities.WorkerUUID | None:
        new_task_uuid = cast(entities.TaskUUID, str(uuid.uuid4()))
        match worker_choose_strategy.strategy:
            case entities.WorkerChooseStrategy.STATIC:
                assert worker_choose_strategy.static_worker is not None, ValueError("static worker must not None")
                return worker_choose_strategy.static_worker
            case entities.WorkerChooseStrategy.RANDOM_FROM_LIST:
                assert worker_choose_strategy.workers is not None and len(worker_choose_strategy.workers) != 0, \
                    ValueError("workers must not None")
                return random.choice(worker_choose_strategy.workers)
            case entities.WorkerChooseStrategy.RANDOM_FROM_WORKER_TAGS:
                assert worker_choose_strategy.worker_tags is not None, ValueError("worker tags should not be None")
                workers = await self.repository.get_workers_from_tags(worker_choose_strategy.worker_tags)
                if workers:
                    choose_worker: entities.Worker = random.choice(workers)
                    return choose_worker.uuid
                else:
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
                        description=cron_task.description,
                        result=f"no worker of tag {worker_choose_strategy.worker_tags}"
                    ))
                    return None

    async def _generate_task_from_argument_strategy(
            self,
            cron_task: entities.CronTask,
            argument_strategy: entities.ArgumentStrategy
    ) -> entities.TaskUUID | None:
        new_task_uuid = cast(entities.TaskUUID, str(uuid.uuid4()))
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
            case _:
                raise NotImplementedError(f"not implement strategy {argument_strategy.strategy}")

    async def _create_cron_sub_task(self, cron_task: entities.CronTask):
        # resolve udf strategy
        argument_strategy = await self._resolve_argument_strategy(
            cron_task.argument_generate_strategy,
            asdict(cron_task)
        )
        # resolve worker choose strategy
        worker_choose_strategy: entities.WorkerStrategy = await self._resolve_worker_choose_strategy(
            cron_task.worker_choose_strategy,
            asdict(cron_task)
        )
        worker: entities.WorkerUUID | None = await self._choose_worker_from_worker_choose_strategy(
            cron_task,
            worker_choose_strategy
        )
        if worker is None:
            return
            # lock worker and check queue status
        with self.lock.lock(worker):
            queue_strategy = await self._resolve_task_queue_strategy(
                cron_task.task_queue_strategy,
                asdict(cron_task)
            )
            # generate task write to db
            new_task_uuid = await self._generate_task_from_argument_strategy(
                cron_task,
                argument_strategy
            )
            # is None means skip or drop task, no need to assign
            if new_task_uuid is None:
                return
            task_queue_size = await self.funtask_manager.get_task_queue_size(worker)
            if task_queue_size > 0:
                await self.assign_task(new_task_uuid)
            else:
                match queue_strategy.full_strategy:
                    case entities.QueueFullStrategy.DROP:
                        ...
                    case entities.QueueFullStrategy.SKIP:
                        await self.repository.change_task_status(
                            task_uuid=new_task_uuid,
                            status=entities.TaskStatus.SKIP
                        )
                    case entities.QueueFullStrategy.SEIZE:
                        await self.assign_task(new_task_uuid)
                    case _:
                        raise NotImplementedError(f"not implemented strategy {queue_strategy.full_strategy}")

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
        tasks = await self.cron.get_all()
        return list(set(_task_from_name(task) for task in tasks))


class LeaderScheduler(interface.LeaderScheduler):
    async def scheduler_node_change(self, scheduler_nodes: List[SchedulerNode]):
        pass
