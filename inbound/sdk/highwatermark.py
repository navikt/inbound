from abc import ABC, abstractmethod


class Highwatermark(ABC):

    @abstractmethod
    def generate_query_list(self) -> list[dict]:
        pass
