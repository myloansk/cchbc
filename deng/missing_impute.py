from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Optional

class NullHandler(ABC):


    @abstractmethod
    def set_next(self,null_handler:NullHandler) -> NullHandler:
        pass
    @abstractmethod
    def handle(self,request)->Optional:
        pass


class AbstractNullHandler(NullHandler):


    _next_handler: NullHandler = None

    def set_next(self,null_handler:NullHandler) -> NullHandler:

        self._next_handler = null_handler

        return null_handler

    @abstractmethod
    def handle(self,request: Any) -> str:
        if(self._next_handler):
            return self._next_handler.handle(request)

        return asNondeterministic


class ZeroImputeHandler(AbstractNullHandler):
    def handle(self,request:Any) -> str:
        pass

class MeanImputeHandler(AbstractNullHandler):
    def handle(self,request:Any) -> str:
        pass

class UnknownImputeHandler(AbstractNullHandler):
    def handle(self,request:Any) -> str:
        pass

class FfillImputeHandler(AbstractNullHandler):
    def handle(self,request:Any) -> str:
        pass

class NfillImputeHandler(AbstractNullHandler):
    def handle(self,request:Any) -> str:
        pass

class BucketImputeHandler(AbstractNullHandler):
    def handle(self,request:Any) -> str:
        pass
