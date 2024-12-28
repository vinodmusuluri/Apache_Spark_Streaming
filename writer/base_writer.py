from abc import ABC, abstractmethod

class BaseWriter(ABC):
    @abstractmethod
    def write(self, data):
        """Abstract method for writing data."""
        pass



