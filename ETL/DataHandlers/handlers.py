from dal import AbstractDataHandler, AbstractDBHandler

class CSVDataHandler(AbstractDataHandler):
    """Handler for CSV data operations"""
    def read(self):
        pass
    
    def write(self, data):
        pass

class XMLDataHandler(AbstractDataHandler):
    """Handler for XML data operations"""
    def read(self):
        pass
    
    def write(self, data):
        pass

class GraphDatabaseHandler(AbstractDBHandler):
    """Handler for Graph database operations"""
    def connect(self):
        pass
    
    def query(self, query):
        pass

class DBHandler(AbstractDBHandler):
    """Handler for relational database operations"""
    def connect(self):
        pass
    
    def query(self, query):
        pass
