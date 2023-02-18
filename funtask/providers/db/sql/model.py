from abc import abstractmethod
from enum import auto, unique
from typing import List, cast, Generic, TypeVar, Protocol, Any, Optional

import dill
from sqlalchemy import Column, Integer, String, ForeignKey, Enum, VARBINARY, Boolean, Float, BigInteger, JSON, \
    TIMESTAMP, UniqueConstraint
from sqlalchemy.orm import declarative_base, relationship

from funtask.core import entities
from funtask.utils.enum_utils import AutoName

Base = declarative_base()


@unique
class ParentTaskType(AutoName):
    TASK = auto()
    CRON_TASK = auto()


@unique
class WorkerStatus(AutoName):
    RUNNING = auto()
    STOPPING = auto()
    STOPPED = auto()
    DIED = auto()


@unique
class TaskStatus(AutoName):
    UNSCHEDULED = auto()
    SCHEDULED = auto()
    SKIP = auto()
    QUEUED = auto()
    RUNNING = auto()
    SUCCESS = auto()
    ERROR = auto()
    DIED = auto()


_T = TypeVar('_T')


class EntityConvertable(Generic[_T]):
    @abstractmethod
    def to_entity(self) -> _T:
        ...


class WithUUID(Protocol):
    uuid: Any


class Worker(Base):
    __tablename__ = 'worker'
    id = Column(Integer(), primary_key=True, autoincrement=True, nullable=False)
    uuid = Column(String(36), nullable=False, unique=True)
    status = Column(Enum(WorkerStatus), nullable=False, index=True)
    name = Column(String(64), nullable=True, index=True)
    last_heart_beat = Column(TIMESTAMP(), nullable=False, index=True)
    start_time = Column(TIMESTAMP(), nullable=False, index=True)
    stop_time = Column(TIMESTAMP(), nullable=True, index=True)
    tags: List['TagRelation'] = relationship(
        'TagRelation',
        primaryjoin='Worker.uuid == foreign(TagRelation.related_uuid)',
        lazy='joined',
        viewonly=True
    )

    def to_entity(self) -> entities.Worker:
        return entities.Worker(
            uuid=cast(entities.WorkerUUID, self.uuid),
            status=entities.WorkerStatus(self.status.value),
            name=self.name,
            tags=TagRelation.tag_relations2entities(self.tags),
            last_heart_beat=self.last_heart_beat,
            start_time=self.start_time,
            stop_time=self.stop_time
        )


class ParameterSchema(Base):
    __tablename__ = 'parameter_schema'
    id = Column(Integer(), primary_key=True,
                autoincrement=True, nullable=False)
    uuid = Column(String(36), nullable=False, unique=True)
    functions: List['Function']

    def to_entity(self) -> entities.ParameterSchema:
        return entities.ParameterSchema(
            uuid=cast(entities.ParameterSchemaUUID, self.uuid)
        )


class Function(Base):
    __tablename__ = 'function'
    id = Column(BigInteger, primary_key=True,
                autoincrement=True, nullable=False)
    uuid = Column(String(36), nullable=False, unique=True)
    parameter_schema_id = Column(
        Integer(),
        ForeignKey(
            'parameter_schema.id'
        ),
        nullable=True
    )
    parameter_schema: ParameterSchema | None = relationship(
        'ParameterSchema',
        backref='functions',
        lazy='joined',
        viewonly=True
    )
    function = Column(VARBINARY(1024), nullable=False)
    name = Column(String(64), nullable=True)
    description = Column(String(256), nullable=True)
    dependencies = Column(JSON, nullable=False)
    ref_tasks: List['Task']
    ref_cron_tasks: List['CronTask']
    tags: List['TagRelation'] = relationship(
        'TagRelation',
        primaryjoin='Function.uuid == foreign(TagRelation.related_uuid)',
        lazy='joined',
        viewonly=True
    )

    def to_entity(self) -> entities.Func:
        parameter_schema = self.parameter_schema
        if parameter_schema is not None:
            parameter_schema = parameter_schema.to_entity()
        return entities.Func(
            uuid=cast(entities.FuncUUID, self.uuid),
            func=self.function,
            dependencies=self.dependencies,
            parameter_schema=parameter_schema,
            description=self.description,
            tags=[],
            name=self.name
        )


class Argument(Base):
    __tablename__ = 'argument'

    id = Column(BigInteger, primary_key=True, autoincrement=True, nullable=False)
    uuid = Column(String(36), nullable=False, unique=True)
    name = Column(String(32), nullable=True)
    argument = Column(VARBINARY(1024), nullable=False)

    def to_entity(self) -> entities.FuncArgument:
        args, kwargs = dill.loads(self.argument)
        return entities.FuncArgument(
            cast(entities.FuncArgumentUUID, self.uuid),
            self.name,
            args,
            kwargs
        )


class Task(Base):
    __tablename__ = 'task'

    id = Column(BigInteger, primary_key=True,
                autoincrement=True, nullable=False)
    uuid = Column(String(36), nullable=False, unique=True)
    uuid_in_manager = Column(String(36), nullable=True)
    parent_task_uuid = Column(String(36), nullable=True)
    parent_task_type = Column(Enum(ParentTaskType), nullable=True)
    worker_id = Column(Integer(), ForeignKey('worker.id'), nullable=True)
    worker: Worker = relationship('Worker', lazy='joined')
    status = Column(Enum(TaskStatus), nullable=False)
    func_id = Column(BigInteger(), ForeignKey('function.id'), nullable=False)
    func: Function = relationship('Function', backref="ref_tasks", lazy='joined')
    argument_id = Column(ForeignKey(Argument.id))
    argument: Argument = relationship('Argument', lazy='joined')
    result_as_state = Column(Boolean(), nullable=False)
    timeout = Column(Float(), nullable=True)
    description = Column(String(256), nullable=True)
    result = Column(String(1024), nullable=True)
    start_time = Column(TIMESTAMP(), nullable=True, index=True)
    stop_time = Column(TIMESTAMP(), nullable=True, index=True)
    name = Column(String(24), nullable=True)

    def to_entity(self) -> entities.Task:
        return entities.Task(
            cast(entities.TaskUUID, self.uuid),
            cast(
                entities.TaskUUID,
                self.parent_task_uuid
            ) if self.parent_task_type == ParentTaskType.TASK else cast(entities.CronTaskUUID, self.parent_task_uuid),
            cast(entities.TaskUUID, self.uuid_in_manager),
            self.status.value,
            cast(entities.WorkerUUID, self.worker.uuid),
            cast(entities.FuncUUID, self.func.uuid),
            self.argument,
            self.result_as_state,
            self.timeout,
            self.description,
            self.result,
            self.name
        )


@unique
class ArgumentStrategy(AutoName):
    STATIC = auto()
    DROP = auto()
    SKIP = auto()
    FROM_QUEUE_END_REPEAT_LATEST = auto()
    FROM_QUEUE_END_SKIP = auto()
    FROM_QUEUE_END_DROP = auto()
    UDF = auto()


class Queue(Base):
    __tablename__ = 'queue'
    id = Column(Integer(), primary_key=True, autoincrement=True, nullable=False)
    uuid = Column(String(36), nullable=False, unique=True)
    name = Column(String(64), nullable=False)  # type: ignore


@unique
class WorkerChooseStrategy(AutoName):
    STATIC = auto()
    RANDOM_FROM_LIST = auto()
    RANDOM_FROM_WORKER_TAGS = auto()
    UDF = auto()


@unique
class QueueFullStrategy(AutoName):
    SKIP = auto()
    DROP = auto()
    SEIZE = auto()
    UDF = auto()


@unique
class TagType(AutoName):
    CRON_TASK = auto()
    WORKER = auto()
    FUNCTION = auto()


class Tag(Base):
    __tablename__ = 'tag'
    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    tag_name = Column(String(16), nullable=False)
    parent_tag_id = Column(Integer, nullable=True, index=True)
    tag_type = Column(Enum(TagType), nullable=False)

    name_parent_uniq = UniqueConstraint(tag_type, tag_name, parent_tag_id)


class TagRelation(Base):
    __tablename__ = 'tag_relation'
    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    tag_id = Column(Integer, ForeignKey('tag.id'), nullable=False)
    tag: Tag = relationship(Tag, lazy='joined')
    related_uuid = Column(String(36), nullable=False)

    uniq = UniqueConstraint(tag_id, related_uuid)

    @staticmethod
    def tag_relations2entities(tag_relations: List['TagRelation']) -> List[entities.Tag]:
        parent_tag2name_map = {
            tag_relation.tag.id: tag_relation.tag.tag_name for tag_relation in tag_relations
            if tag_relation.tag.parent_tag_id is None
        }
        return [
            entities.Tag(
                key=parent_tag2name_map[tag_relation.tag.parent_tag_id],
                value=tag_relation.tag.tag_name,
            ) for tag_relation in tag_relations if tag_relation.tag.parent_tag_id is not None
        ]


class CronTask(Base):
    __tablename__ = 'cron_task'
    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    uuid = Column(String(36), nullable=False, unique=True)
    name = Column(String(64), nullable=False)
    function_id = Column(
        BigInteger(),
        ForeignKey('function.id'),
        nullable=False
    )
    timepoints = Column(JSON, nullable=False)
    argument_generate_strategy = Column(Enum(ArgumentStrategy), nullable=False)
    argument_generate_strategy_static_argument_id = Column(ForeignKey('argument.id'), nullable=True)
    argument_generate_strategy_static_argument: Optional[Argument] = relationship(
        'Argument',
        foreign_keys=[argument_generate_strategy_static_argument_id],
        lazy='joined'
    )
    argument_generate_strategy_args_queue_id = Column(
        Integer(),
        ForeignKey('queue.id'),
        nullable=True
    )
    argument_generate_strategy_udf_id = Column(
        BigInteger(),
        ForeignKey('function.id'),
        nullable=True
    )
    argument_generate_strategy_udf_extra = Column(
        JSON(),
        nullable=True
    )
    worker_choose_strategy = Column(Enum(WorkerChooseStrategy), nullable=False)
    worker_choose_strategy_static_worker_id = Column(
        Integer(),
        ForeignKey('worker.id'),
        nullable=True
    )
    worker: Worker = relationship(
        'Worker',
        foreign_keys=[worker_choose_strategy_static_worker_id],
        lazy="joined"
    )
    worker_choose_strategy_worker_tags = Column(
        JSON(none_as_null=False), nullable=True)
    worker_choose_strategy_udf_id = Column(
        BigInteger,
        ForeignKey('function.id'),
        nullable=True
    )
    worker_choose_strategy_udf: Function = relationship(
        'Function',
        foreign_keys=[worker_choose_strategy_udf_id],
        lazy="joined"
    )

    worker_choose_strategy_udf_extra = Column(JSON(), nullable=True)
    task_queue_strategy = Column(Enum(QueueFullStrategy), nullable=False)
    task_queue_max_size = Column(BigInteger(), nullable=False)
    task_queue_strategy_udf_id = Column(
        BigInteger(),
        ForeignKey('function.id'),
        nullable=True
    )
    task_queue_strategy_udf: Function = relationship(
        'Function',
        foreign_keys=[task_queue_strategy_udf_id],
        lazy="joined"
    )
    task_queue_strategy_udf_extra = Column(JSON(), nullable=True)
    result_as_status = Column(Boolean(), nullable=False)
    timeout = Column(Float(), nullable=True)
    description = Column(String(256), nullable=True)
    disabled = Column(Boolean(), nullable=False)
    func: Function = relationship(
        'Function',
        foreign_keys=[function_id],
        backref="ref_corn_tasks",
        lazy="joined"
    )
    tags: List['TagRelation'] = relationship(
        'TagRelation',
        primaryjoin='CronTask.uuid == foreign(TagRelation.related_uuid)',
        lazy="joined"
    )

    def to_entity(self) -> entities.CronTask:
        return entities.CronTask(
            uuid=cast(entities.CronTaskUUID, self.uuid),
            timepoints=[entities.TimePoint(**kwargs) for kwargs in self.timepoints],
            func=self.func.to_entity(),
            argument_generate_strategy=entities.ArgumentStrategy(
                strategy=self.argument_generate_strategy,
                static_argument=self.argument_generate_strategy_static_argument and self.argument_generate_strategy_static_argument.to_entity(),
                argument_queue=None,
                udf=self.func.to_entity(),
                udf_extra=self.argument_generate_strategy_udf_extra
            ),
            worker_choose_strategy=entities.WorkerStrategy(
                strategy=self.worker_choose_strategy,
                static_worker=cast(entities.WorkerUUID, self.worker.uuid),
                worker_tags=[entities.Tag(**kwargs) for kwargs in self.worker_choose_strategy_worker_tags],
                udf=self.worker_choose_strategy_udf,
                udf_extra=self.worker_choose_strategy_udf_extra
            ),
            task_queue_strategy=entities.QueueStrategy(
                full_strategy=self.task_queue_strategy,
                udf=self.task_queue_strategy_udf,
                udf_extra=self.task_queue_strategy_udf_extra,
                max_size=self.task_queue_max_size
            ),
            result_as_state=self.result_as_status,
            timeout=self.timeout,
            disabled=self.disabled,
            name=self.name,
            description=self.description,
            tags=[tag_relation.tag for tag_relation in self.tags]
        )
