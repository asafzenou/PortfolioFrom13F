from abc import abstractmethod
from typing import Any, List
from DataHandlers.base_handler import AbstractDataHandler


class AbstractDBHandler(AbstractDataHandler):

    @abstractmethod
    def connect(self) -> None:
        pass

    @abstractmethod
    def query(self, sql: str) -> List[Any]:
        pass

    @abstractmethod
    def execute(self, sql: str, params: tuple | None = None) -> None:
        pass

    def read(self) -> List[Any]:
        raise NotImplementedError("Database handlers do not use read(); use query().")
