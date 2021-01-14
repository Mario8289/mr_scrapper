from abc import ABC, abstractmethod
from .scrap import Scrap


class Job(ABC):
    def __init__(self, scrap: Scrap, dao):
        self.scrap = scrap
        self.dao = dao

    @abstractmethod
    def run(self, *args, **kwargs):
        raise NotImplementedError("you need a run method on your job, or it won't do a thing, silly!")
