from abc import abstractmethod
from enum import auto, unique
from typing import List, cast, Generic, TypeVar, Protocol, Any, Optional
from sqlalchemy import Column, Integer, String, ForeignKey, Enum, VARBINARY, Boolean, Float, BigInteger, JSON, \
    TIMESTAMP, UniqueConstraint
from sqlalchemy.orm import declarative_base, relationship, Mapped

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


class Namespace(Base):
    __tablename__ = 'namespace'
    id = Column(Integer(), primary_key=True, autoincrement=True, nullable=False)
    name = Column(String(32), nullable=False, unique=True)


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
        'Tag',
        primaryjoin='Worker.uuid == foreign(Tag.related_uuid)',
        backref='Workers'
    )

    def to_entity(self) -> _T:
        return entities.Worker(
            uuid=cast(entities.WorkerUUID, self.uuid),
            status=entities.WorkerStatus(self.status.value),
            name=self.name,
            tags=TagRelation.tag_relations2entities(self.tags)
        )


class ParameterSchema(Base):
    __tablename__ = 'parameter_schema'
    id = Column(Integer(), primary_key=True,
                autoincrement=True, nullable=False)
    uuid = Column(String(36), nullable=False, unique=True)
    functions: List['Function']

    def to_entity(self) -> _T:
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
        backref='functions'
    )
    function = Column(VARBINARY(1024), nullable=False)
    name = Column(String(64), nullable=True)
    namespace_id = Column(Integer, ForeignKey('namespace.id'), nullable=False)
    description = Column(String(256), nullable=True)
    dependencies = Column(JSON, nullable=False)
    ref_tasks: List['Task']
    ref_cron_tasks: List['CronTask']

    def to_entity(self) -> _T:
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

    def to_entity(self) -> _T:
        pass


class Task(Base):
    __tablename__ = 'task'

    id = Column(BigInteger, primary_key=True,
                autoincrement=True, nullable=False)
    uuid = Column(String(36), nullable=False, unique=True)
    uuid_in_manager = Column(String(36), nullable=False)
    parent_task_uuid = Column(String(36), nullable=True)
    parent_task_type = Column(Enum(ParentTaskType), nullable=True)
    worker_id = Column(Integer(), ForeignKey('worker.id'), nullable=True)
    worker: Worker = relationship('Worker')
    status = Column(Enum(TaskStatus), nullable=False)
    func_id = Column(BigInteger(), ForeignKey('function.id'), nullable=False)
    func: Function = relationship('Function', backref="ref_tasks")
    argument = Column(VARBINARY(1024), nullable=False)
    result_as_state = Column(Boolean(), nullable=False)
    timeout = Column(Float(), nullable=True)
    description = Column(String(256), nullable=True)
    result = Column(String(1024), nullable=False)
    start_time = Column(TIMESTAMP(), nullable=False, index=True)
    stop_time = Column(TIMESTAMP(), nullable=False, index=True)
    namespace_id = Column(Integer, ForeignKey(Namespace.id), nullable=False)
    namespace: Namespace = relationship('Namespace')

    def to_entity(self) -> _T:
        pass


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
    namespace_id = Column(Integer, ForeignKey('namespace.id'), nullable=False)
    namespace: Namespace = relationship(Namespace)
    tag_type = Column(Enum(TagType), nullable=False)

    name_parent_uniq = UniqueConstraint(tag_type, tag_name, parent_tag_id)


class TagRelation(Base):
    __tablename__ = 'tag_relation'
    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    tag_id = Column(Integer, ForeignKey('tag.id'), nullable=False)
    tag: Tag = relationship(Tag)
    related_uuid = Column(String(36), nullable=False)

    uniq = UniqueConstraint(tag_id, related_uuid)

    worker: Mapped[Optional[Worker]] = relationship(
        'Worker',
        foreign_keys=[related_uuid],
        primaryjoin='foreign(Worker.uuid) == Tag.related_uuid',
        backref='ref_tags'
    )

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
                namespace=tag_relation.tag.namespace.name
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
    argument_generate_strategy = Column(Enum(ArgumentStrategy), nullable=False)
    argument_generate_strategy_static_value_id = Column(
        BigInteger(), nullable=True)
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
    worker_choose_strategy_worker_uuid_list = Column(
        JSON(none_as_null=False), nullable=True)
    worker_choose_strategy_worker_tags = Column(
        JSON(none_as_null=False), nullable=True)
    worker_choose_strategy_udf_id = Column(
        BigInteger,
        ForeignKey('function.id'),
        nullable=True
    )
    worker_choose_strategy_udf_extra = Column(JSON(), nullable=True)
    task_queue_strategy = Column(Enum(QueueFullStrategy), nullable=False)
    task_queue_max_size = Column(BigInteger(), nullable=False)
    task_queue_strategy_udf_id = Column(
        BigInteger(),
        ForeignKey('function.id'),
        nullable=True
    )
    task_queue_strategy_udf_extra = Column(JSON(), nullable=True)
    result_as_status = Column(Boolean(), nullable=False)
    timeout = Column(Float(), nullable=True)
    description = Column(String(256), nullable=True)
    disabled = Column(Boolean(), nullable=False)
    func: Function = relationship(
        'Function',
        foreign_keys=[function_id],
        backref="ref_corn_tasks"
    )

    def to_entity(self) -> _T:
        pass
