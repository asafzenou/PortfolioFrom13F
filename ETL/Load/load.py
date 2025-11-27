from typing import Any, List
from abc import ABC, abstractmethod

class AbstractLoad(ABC):
    """Abstract base class for data loading"""
    @abstractmethod
    def load(self, data: List[Any]) -> None:
        pass

class Load(AbstractLoad):
    """Generic data loader that works with any handler"""
    def __init__(self, handler):
        """
        Initialize loader with a data handler
        
        Args:
            handler: Any data handler (CSVDataHandler, XMLDataHandler, DBHandler, etc.)
        """
        self.handler = handler
    
    def load(self, data: List[Any]) -> None:
        """Load data using the provided handler"""
        if data is None:
            print("Warning: No data to load")
            return
        
        try:
            self.handler.write(data)
            print(f"Data successfully loaded via {self.handler.__class__.__name__}")
        except Exception as e:
            print(f"Error loading data: {e}")
