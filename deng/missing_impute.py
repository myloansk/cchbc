from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Optional,List

class NullHandler(ABC):

    @abstractmethod
    def set_next(self,null_handler:NullHandler) -> NullHandler:
        pass
    @abstractmethod
    def handle(self,impute_method,sdf,colList):
        pass
class AbstractNullHandler(NullHandler):

    _next_handler: NullHandler = None

    def set_next(self,null_handler:NullHandler) -> NullHandler:
        self._next_handler = null_handler
        return null_handler

    @abstractmethod
    def handle(self,impute_method,sdf,colList):
        if self._next_handler:
            return self._next_handler.handle(impute_method,sdf,colList)
        return sdf


class ZeroImputeHandler(AbstractNullHandler):
    def handle(self,impute_method,sdf,colList):
        if impute_method == '0':
                for colName in colList:
                    sdf = sdf.withColumn(colName,f.when(f.col(colName).isNull(),f.lit(0)).otherwise(f.col(colName)))
                return sdf
            else:
                return  super().handle(impute_method,sdf,colList)
class MeanImputeHandler(AbstractNullHandler):
    def handle(self,impute_method,sdf,colList):
        pass

class UnknownImputeHandler(AbstractNullHandler):
    def handle(self,impute_method,sdf,colList):
        pass
class FfillImputeHandler(AbstractNullHandler):
    def handle(self,impute_method,sdf,colList):
        pass

class NfillImputeHandler(AbstractNullHandler):
    def handle(self,impute_method,sdf,colList):
        pass
class BucketImputeHandler(AbstractNullHandler):
    def handle(self,impute_method,sdf,colList):
        pass
