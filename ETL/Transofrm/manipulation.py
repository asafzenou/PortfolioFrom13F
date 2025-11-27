from typing import Any, List
from abc import ABC, abstractmethod

class AbstractManipulation(ABC):
    """Abstract base class for data manipulation"""
    @abstractmethod
    def process(self, data: List[Any]) -> List[Any]:
        pass

class FilterManipulation(AbstractManipulation):
    """Filter data based on criteria"""
    def __init__(self, criteria: callable):
        self.criteria = criteria
    
    def process(self, data: List[Any]) -> List[Any]:
        """Filter data using provided criteria"""
        return [item for item in data if self.criteria(item)]

class TransformManipulation(AbstractManipulation):
    """Transform data structure or values"""
    def __init__(self, transformer: callable):
        self.transformer = transformer
    
    def process(self, data: List[Any]) -> List[Any]:
        """Transform each data item"""
        return [self.transformer(item) for item in data]

class AggregateManipulation(AbstractManipulation):
    """Aggregate data"""
    def __init__(self, aggregator: callable, initial_value: Any = None):
        self.aggregator = aggregator
        self.initial_value = initial_value
    
    def process(self, data: List[Any]) -> List[Any]:
        """Aggregate data items"""
        result = self.initial_value
        for item in data:
            result = self.aggregator(result, item)
        return [result] if result is not None else []

class ChainManipulation(AbstractManipulation):
    """Chain multiple manipulation operations"""
    def __init__(self, manipulations: List[AbstractManipulation]):
        self.manipulations = manipulations
    
    def process(self, data: List[Any]) -> List[Any]:
        """Apply manipulations in sequence"""
        result = data
        for manipulation in self.manipulations:
            result = manipulation.process(result)
        return result
