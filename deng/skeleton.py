from __future__ import annotations
from dataclasses import dataclass
from functools import reduce
from abc import ABC, abstractmethod

import pandas as pd
from pyspark.sql import DataFrame
from typing import Dict,List,Optional,Any


@dataclass
class dataContainer:
    """
    Data driven class serves as container of data that can be used across the 
    projects in CCHBC encapsulating reccuring needs for analyzing, validating and combining data

    There two kinds of classes that the reader may come across in CCHBC
    - Behaviour Driven classes
    - Data Data Driven classes
    Although both of them are classes, they serve different purpose and utilization.
    Here, the focus is shifted to the latter type of classes and the lack
    of out-of-the box functionality of bare metal python classes to adapt
    to the ways the data driven classes are ment to be used inside the code
    thus leading to coupling and low cohesion.
    So, we have compiled a minimilstic blueprint that resembles that of a struct
    so the future dev can leverage the work of the past without having 
    

    Arguments
    @dataDf:pyspark.DataFrame
    @configs:dictionary containing set of instructions with the steps of transformations and configuration that will be used along the way 
    """
    dataDf:DataFrame
    configs:Dict

class Process(ABC):
    """
    The Process interface declares a number of methods 
    that encapsulate basic functionalities ubiquitous 
    in CCHBC projects and can be further augmented/twinkled 
    by concrete subclasses depending on the needs of the project.
    It also defines a method (assemble) that accomodates the skeleton of
    some algorithm, composed of calls to (usually) abstract primitive
    operations.
    """

    def assemble(self)->None:
        """
        The assemble defines the skeleton of an algorithm.
        """
        pass

class stageHandler(ABC):
    """
    The stageHandler interface declares a method for building the chain of handlers.
    It also declares a method for executing a request.
    """
    @abstractmethod
    def add_stage(self, stage: stageHandler) -> stageHandler:
        pass

    @abstractmethod
    def process(self, request) -> Optional[str]:
        pass

class abstractStageHandler(stageHandler):
    """
    The default chaining behavior can be implemented inside a base handler
    class.
    """
    _this_stage: stageHandler = None
    
    def add_stage(self, stage: stageHandler) -> stageHandler:
        self._this_stage = stage
        return stage
    
    @abstractmethod
    def process(self,request: Any) -> str:
        if self._this_stage:
            return self._this_stage.process(request)
        return None