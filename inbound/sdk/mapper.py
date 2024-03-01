from abc import ABC, abstractmethod

from inbound.core.models import Description


class Mapper(ABC):
    @abstractmethod
    def map(self, column_description: Description) -> Description:
        pass
