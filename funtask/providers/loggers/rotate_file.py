import copy
import multiprocessing
import sys
from contextlib import asynccontextmanager
from io import StringIO
from pathlib import Path
from typing import List, Iterable, Tuple
from abc import abstractmethod

from dependency_injector.wiring import Provide

from funtask.core import entities
from funtask.core.interface_and_types import Logger, LogLevel, LogType

Offset = Tuple[str, int]  # [file_name, file_offset]


class Repository:
    @abstractmethod
    async def set_task_log_start(self, task_uuid: entities.TaskUUID, offset: Offset, session=None):
        ...

    @abstractmethod
    async def set_task_log_end(self, task_uuid: entities.TaskUUID, offset: Offset, session=None):
        ...

    @abstractmethod
    async def get_task_log_offset_info(
            self,
            task_uuid: entities.TaskUUID,
            session=None
    ) -> Tuple[Offset, Offset | None]:
        """
        get task start and end offset
        :param task_uuid: uuid of task
        :param session: repository session if supported, default None
        :return start and end offset, if not end, end offset is None
        """
        ...

    @abstractmethod
    async def create_model_schema(self):
        pass

    @abstractmethod
    async def drop_model_schema(self):
        pass


class HookedIO(StringIO):
    def __init__(self, worker_uuid: entities.WorkerUUID, log_dir: Path, rotate_line_num: int):
        super().__init__()
        self.worker_uuid = worker_uuid
        self.curr_fid = 0
        self.log_lock = multiprocessing.Lock()
        self.curr_max_line = 0
        self.rotate_line_num = rotate_line_num
        log_dir.mkdir(exist_ok=True)
        self.log_dir = log_dir
        self.log_f = self._mk_log_file(self.curr_fid)

    def _mk_log_file(self, curr_fid: int):
        return open(self.log_dir / f'{self.worker_uuid}-{curr_fid}.log', 'w')

    def __del__(self):
        self.log_f.close()

    def write(self, __s: str) -> int:
        with self.log_lock:
            self.log_f.write(__s)
            self.log_f.flush()
            self.curr_max_line += 1
            if self.curr_max_line >= self.rotate_line_num:
                self.curr_max_line = 0
                self.log_f.close()
                self.curr_fid += 1
                self.log_f = self._mk_log_file(self.curr_fid)
        return len(__s)

    def writable(self) -> bool:
        return True

    def write_through(self) -> bool:
        return True

    def writelines(self, __lines: Iterable[str]) -> None:
        for line in __lines:
            self.write(line)

    def seekable(self) -> bool:
        return False

    def get_pos(self) -> Offset:
        """
        get position of log file
        :return: [file_id, file_offset]
        """
        return f'{self.worker_uuid}-{self.curr_fid}.log', self.log_f.tell()


class RotateFileLogger(Logger):
    def __init__(
            self,
            repository: Repository = Provide['logger.repository'],
            log_dir: str = Provide['logger.log_dir'],
            rotate_line_num: int = Provide['logger.rotate_line_num']
    ):
        self.is_io_hooked = False
        self.worker_uuid: entities.WorkerUUID | None = None
        self.task_uuid: entities.TaskUUID | None = None
        self.hooked_io: HookedIO | None = None
        self.log_dir = Path(log_dir)
        self.rotate_line_num = rotate_line_num
        self.repository = repository

    async def with_worker(self, worker_uuid: entities.WorkerUUID) -> 'RotateFileLogger':
        # deep copy the entire logger, without repository
        repository = self.repository
        self.repository = None
        logger_cpy = copy.deepcopy(self)
        logger_cpy.repository = repository
        logger_cpy.worker_uuid = worker_uuid
        logger_cpy.hooked_io = HookedIO(
            worker_uuid,
            self.log_dir,
            rotate_line_num=self.rotate_line_num
        )
        return logger_cpy

    async def with_task(self, task_uuid: entities.TaskUUID) -> 'RotateFileLogger':
        # shallow copy with same hooked_io
        logger_cpy = copy.copy(self)
        self.task_uuid = task_uuid
        return logger_cpy

    async def create_model_schema(self):
        await self.repository.create_model_schema()

    async def drop_model_schema(self):
        await self.repository.drop_model_schema()

    @asynccontextmanager
    async def task_context(self):
        assert self.hooked_io, AssertionError('hooked io is None, worker uuid must be set before log')
        assert self.task_uuid, AssertionError('task_uuid is None, task uuid must be set before start context')
        file_id, file_offset = self.hooked_io.get_pos()
        await self.repository.set_task_log_start(self.task_uuid, (file_id, file_offset))
        try:
            yield
        finally:
            file_id, file_offset = self.hooked_io.get_pos()
            await self.repository.set_task_log_end(self.task_uuid, (file_id, file_offset))

    def _hook_io(self):
        if not self.is_io_hooked:
            sys.stdout = self.hooked_io

    async def log(self, msg: str, level: LogLevel, tags: List[str], type_: LogType = LogType.TEXT):
        assert self.hooked_io, AssertionError('hooked io is None, worker uuid must be set before log')
        self._hook_io()
        tags = tags or ["default"]
        print(f"{level.value}:{self.worker_uuid}-{tags}: {msg}")
