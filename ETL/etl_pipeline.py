from typing import List, Any
from collections import deque
from ETL.Extractors.extractor import AbstractExtractor
from ETL.Transofrm.manipulation import AbstractManipulation
from ETL.Load.load import AbstractLoad

class Queue:
    """Simple queue implementation for pipeline stages"""
    def __init__(self) -> None:
        self.items: deque = deque()

    def enqueue(self, item: Any) -> None:
        self.items.append(item)

    def dequeue(self) -> Any:
        return self.items.popleft() if self.items else None

class Pipeline:
    """Main processing pipeline orchestrator"""
    def __init__(
        self,
        extractor: AbstractExtractor,
        manipulation: AbstractManipulation,
        load: AbstractLoad
    ) -> None:
        self.extractor = extractor
        self.manipulation = manipulation
        self.load = load
        self.input_queue = Queue()
        self.output_queue = Queue()

    def run(self) -> None:
        raw_data = self.extractor.extract()
        self.input_queue.enqueue(raw_data)

        processed_data = self.manipulation.process(self.input_queue.dequeue())
        self.output_queue.enqueue(processed_data)

        self.load.load(self.output_queue.dequeue())
