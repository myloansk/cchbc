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

from pyspark.sql import DataFrame,Row
from pyspark.ml.linalg import (SparseVector,
                               DenseVector,
                               Vectors,
                               VectorUDT)

from pyspark.ml.feature import (OneHotEncoder,
                                ChiSqSelector,
                                StringIndexer,
                                VectorAssembler,
                                BucketedRandomProjectionLSH,
                                VectorSlicer)


from pyspark.ml import Pipeline



class ImbalanceHandler(ABC):
  """
  Declares an interface for handling imbalance in Pyspark Dataframes

  [Declares a common interface to each implementation of algorithms
  for handling imbalance in pyspark Dataframe that are defined below]

  :param ABC: [description]
  :type ABC: [type]
  """
  @abstractmethod
  def get_samples(self)->None:
      pass

class ImbalanceHandlerReceiver():
    """

    """

class ImbalanceMethodInvoker:
    """

    """

    def __init__(self):
        self._sampler = None
        self._receiver =None

    def sample(self,sampler:ImbalanceHandler)->None:
        self._sampler = sampler

    def get_samples(self):
        self._sampler.get_samples()
