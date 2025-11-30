# from typing import Any, List
# from base_strategy import ExtractionStrategy
#
#
# class ExtractorContext:
#     """
#     Context that receives a strategy (Extractor)
#     and executes it uniformly.
#     """
#
#     def __init__(self, strategy: ExtractionStrategy):
#         self.strategy = strategy
#
#     def set_strategy(self, strategy: ExtractionStrategy):
#         self.strategy = strategy
#
#     def execute(self) -> List[Any]:
#         return self.strategy.extract()
