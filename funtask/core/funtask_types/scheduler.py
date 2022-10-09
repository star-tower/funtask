from abc import abstractmethod


class Scheduler:
    @abstractmethod
    def create_worker(self) -> str:
        """
        create a worker, return it's uuid
        """
        ...
