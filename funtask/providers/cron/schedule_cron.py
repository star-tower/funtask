import time
from typing import List, Callable, Dict, Any
import schedule
from multiprocessing import Process

from funtask.core import interface_and_types as interface


class SchedulerCron(interface.Cron):
    def __init__(self):
        self.scheduler = schedule.Scheduler()
        self.ms_tasks: Dict[str, (int, Callable, List[Any], Dict[str, Any])] = {}
        self._keep_run = True
        process = Process(target=self._run)
        process.start()

    def __del__(self):
        self._keep_run = False

    def _check_and_run(self, ms: int):
        for run_ms, fun, args, kwargs in self.ms_tasks.values():
            if ms % run_ms:
                fun(*args, **kwargs)

    def _run(self):
        ms_count = 1
        t = time.time()
        while self._keep_run:
            curr_time = time.time()

            # count ms from start for ms schedule
            if curr_time - t > .001:
                t = curr_time
                ms_count += 1

                self._check_and_run(ms_count)

            schedule.run_pending()
            time.sleep(.0002)

    async def get_all(self) -> List[str]:
        tasks = self.scheduler.get_jobs()
        return [str(list(task.tags)[0]) for task in tasks] + list(self.ms_tasks.keys())

    async def every_n_seconds(self, name: str, n: int, task: Callable, at: str | None = None, *args, **kwargs):
        await self.cancel(name)
        task = self.scheduler.every(n).second.at(at).do(task, *args, **kwargs)
        task.tag(name)

    async def every_n_minutes(self, name: str, n: int, task: Callable, at: str | None = None, *args, **kwargs):
        await self.cancel(name)
        task = self.scheduler.every(n).minute.at(at).do(task, *args, **kwargs)
        task.tag(name)

    async def every_n_hours(self, name: str, n: int, task: Callable, at: str | None = None, *args, **kwargs):
        await self.cancel(name)
        task = self.scheduler.every(n).hour.at(at).do(task, *args, **kwargs)
        task.tag(name)

    async def every_n_days(self, name: str, n: int, task: Callable, at: str | None = None, *args, **kwargs):
        await self.cancel(name)
        task = self.scheduler.every(n).day.at(at).do(task, *args, **kwargs)
        task.tag(name)

    async def every_n_weeks(self, name: str, n: int, task: Callable, at: str | None = None, *args, **kwargs):
        await self.cancel(name)
        task = self.scheduler.every(n).week.at(at).do(task, *args, **kwargs)
        task.tag(name)

    async def every_n_millisecond(self, name: str, n: int, task: Callable, *args, **kwargs):
        await self.cancel(name)
        self.ms_tasks[name] = (n, task, args, kwargs)

    async def cancel(self, name: str):
        for job in self.scheduler.get_jobs(name):
            self.scheduler.cancel_job(job)

        self.ms_tasks.pop(name, None)
