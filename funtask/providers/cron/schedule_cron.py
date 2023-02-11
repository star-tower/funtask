import asyncio
import time
from typing import List, Callable, Dict, Any
import schedule
from threading import Thread

from funtask.core import interface_and_types as interface


class SchedulerCron(interface.Cron):
    def __init__(self):
        self.scheduler = schedule.Scheduler()
        self.ms_tasks: Dict[str, (int, Callable, List[Any], Dict[str, Any])] = {}
        self.seconds_tasks: Dict[str, (int, Callable, List[Any], Dict[str, Any])] = {}
        self._keep_run = True

    def __del__(self):
        self._keep_run = False

    async def run(self):
        ms_count = 1
        s_count = 1
        t_ms = time.time()
        t_s = time.time()
        while self._keep_run:
            curr_time = time.time()

            # count ms from start for ms schedule
            if curr_time - t_ms > .001:
                t_ms = curr_time
                ms_count += 1

                for run_ms, fun, args, kwargs in self.ms_tasks.values():
                    if t_ms % run_ms:
                        await fun(*args, **kwargs)

            # count ms from start for seconds schedule
            if curr_time - t_s > 1:
                t_s = curr_time
                s_count += 1

                for run_s, fun, args, kwargs in self.seconds_tasks.values():
                    if t_s % run_s:
                        await fun(*args, **kwargs)

            schedule.run_pending()
            await asyncio.sleep(.001)

    async def get_all(self) -> List[str]:
        tasks = self.scheduler.get_jobs()
        return [str(list(task.tags)[0]) for task in tasks] + list(self.ms_tasks.keys())

    async def every_n_seconds(self, name: str, n: int, task: Callable, at: str | None = None, *args, **kwargs):
        await self.cancel(name)
        self.seconds_tasks[name] = (n, task, args, kwargs)

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
