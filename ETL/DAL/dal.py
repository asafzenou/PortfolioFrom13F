from abc import ABC, abstractmethod

class AbstractDataHandler(ABC):
    """Abstract base handler for data operations"""
    @abstractmethod
    def read(self):
        pass
    
    @abstractmethod
    def write(self, data):
        pass

class AbstractDBHandler(ABC):
    """Abstract base handler for database operations"""
    @abstractmethod
    def connect(self):
        pass
    
    @abstractmethod
    def query(self, query):
        pass

class DAL:
    """Data Access Layer using Strategy Pattern"""
    def __init__(self, data_handler: AbstractDataHandler, db_handler: AbstractDBHandler):
        self.data_handler = data_handler
        self.db_handler = db_handler
    
    def get_data(self):
        return self.data_handler.read()
    
    def persist_data(self, data):
        self.data_handler.write(data)
    
    def query_database(self, query):
        return self.db_handler.query(query)
