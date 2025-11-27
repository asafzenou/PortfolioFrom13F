from neo4j import GraphDatabase
from typing import Any, List
from DataHandlers.DBDataHandler.db_abstract import AbstractDBHandler


class GraphDBHandler(AbstractDBHandler):

    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def connect(self):
        pass  # Neo4j driver handles connection lazily

    def query(self, cypher: str) -> List[Any]:
        with self.driver.session() as session:
            result = session.run(cypher)
            return [record.data() for record in result]

    def execute(self, cypher: str, params=None):
        with self.driver.session() as session:
            session.run(cypher, params or {})

    def close(self):
        self.driver.close()
