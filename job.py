from datetime import datetime
import pickle
from typing import Any, Callable, Tuple
import uuid

from config.logger import logger

class Job:
    def __init__(self,
                 func: Callable,
                 args: Tuple[Any],
                 start_at: datetime = None,
                 max_working_time: int = -1,
                 tries: int = 0,
                 dependencies = None,
                 ):
        self.__func = func
        self.__args = args
        self.__id = uuid.uuid4()
        self.__paused = False
        self.__start_at = int(start_at.timestamp()) if start_at else None
        self.path = None
        self.__max_working_time = max_working_time
        self.__tries = tries
        self.__dependencies = dependencies if dependencies else []

    @property
    def id(self):
        return self.__id

    @property
    def paused(self):
        return self.__paused

    @property
    def is_delayed(self):
        return self.__start_at is not None

    @property
    def start_at(self):
        return self.__start_at

    @property
    def max_working_time(self):
        return self.__max_working_time

    @property
    def tries(self):
        return self.__tries

    def serialize(self):
        return pickle.dumps(self)

    @staticmethod
    def deserialize(data: Any):
        return pickle.loads(data)

    def run(self):
        logger.info(f'Run job [{self.__id}] "{self.__func.__name__}" with params: {self.__args}')
        return self.__func(*self.__args)

    def pause(self):
        self.__paused = True

    def stop(self):
        pass
