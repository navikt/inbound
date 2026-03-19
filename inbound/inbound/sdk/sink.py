from abc import ABC, abstractmethod
from typing import Any, Generator

from ..core.models import Description


class Sink(ABC):
    @abstractmethod
    def ingest(
        self,
        data_generator: Generator[list[tuple], Any, None],
        column_description: list[Description],
    ):
        pass
