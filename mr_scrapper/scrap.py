from abc import ABC, abstractmethod
from typing import AnyStr


class Scrap(ABC):

    @abstractmethod
    def run(self, *args, **kwargs):
        raise NotImplementedError("you can't scrap without a run method on your job boyo!")

    @abstractmethod
    def fetch(self, url: AnyStr):
        raise NotImplementedError("how are you supposed to run a scrapping job without a valid URL, try again :-(")

    @abstractmethod
    def parse(self, data, **kwargs):
        raise NotImplementedError("so you have the html but you now need to create a parse method to mung data into " \
                                  "a form that makes sense, off you go.")
