from __future__ import annotations
from abc import ABC, abstractmethod
import numpy as np
import random
import re
import os
from functools import reduce

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.window import Window

from pyspark.ml.feature import (OneHotEncoder,
                                ChiSqSelector,
                                StringIndexer,
                                VectorAssembler,
                                VectorSlicer)


from pyspark.ml import Pipeline

class DataCleaner():
    """DataCleaner class """

  def __init__(self,method:Method) -> None:
      self._method = method
 @property
 def method(self) -> Method:

    return self._method

 @method.setter
 def method(self,method:Method) ->None:
     self._method = method

class Correlation(ABC):
    @abstractmethod
    def run(self,Dframe,params:Dictionary):
        pass
