from abc import ABC, abstractmethod

class BaseConsumer(ABC):
    def __init__(self, config):
        self.config = config

    @abstractmethod
    def read_stream(self):
        """Abstract method for consuming messages."""
        pass
