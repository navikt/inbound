from abc import ABC, abstractmethod
from typing import Any, Generator

from ..core.models import Description


class Tap(ABC):

    @abstractmethod
    def column_descriptions(self) -> list[Description]:
        pass

    @abstractmethod
    def data_generator(self) -> Generator[list[tuple], Any, None]:
        pass
