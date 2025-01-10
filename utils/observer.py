import abc


class Observer(abc.ABC):
    @abc.abstractmethod
    def update(self, topic: str, message: str):
        """Metoda Observer"""
        pass