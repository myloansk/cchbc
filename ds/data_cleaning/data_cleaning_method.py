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
    """
    Correlation class provides an interface
    for calculating corrleation based on different algorithms
    """
    @abstractmethod
    def apply(self,Dframe,params:Dictionary):
        pass

class DataSelect(ABC):
    """
    The DataSelect class provides an interface of
    different methods to select data from
    pyspark.DataFrame
    """
    @abstractmethod
    def apply(self,Dframe,params:Dictionary):
        pass

class DataSelectDelegatesToBDPEDataFilter(DataSelect):
    @abstractmethod
    def apply(self,Dframe):
        pass

class DataSelectDelegatesToDataSlice(DataSelect):
    @abstractmethod
    def apply(self,Dframe,params:Dictionary):
        Dframe = Dframe.filter(f.col('year_month').isin(params[4])).drop('year')

class SplitTrainTest(ABC):
    """
    SplitTrainTest split DataFrame to train/test setter
    """
    @abstractmethod
    def apply(self,Dframe,params:Dictionary):
        pass

class BDPESplitTrainTest(SplitTrainTest):
    column_names =  AbstractDataPipe.Dframe.columns
    AbstractDataPipe.model_input_columns =  [item for item in column_names if item not in AbstractDataPipe.pipeline_param_dictionary[8] and '=Unknown' not in item]

    bd_ids = [i.employee_id for i in AbstractDataPipe.Dframe.select('employee_id').distinct().collect()]

    df = AbstractDataPipe.Dframe

    w = Window.partitionBy(f.col('employee_id')).orderBy(f.col('year_month').cast('integer').desc())
    df = df.withColumn('isLatest', f.row_number().over(w)).filter(f.col('isLatest')==1).drop('isLatest')\
           .filter(f.col('employee_id').isin(bd_ids)).select(f.col('employee_id'),
                                                          f.col('voluntary_leave_6MAfter'))

    leaver_ids =[i.employee_id for i in df.filter(f.col('voluntary_leave_6MAfter')==1).select('employee_id').distinct().collect()]
    stayers_ids  =[i.employee_id for i in df.filter(f.col('voluntary_leave_6MAfter').isin([9999,0])).select('employee_id').distinct().collect()]


    leavers_size_population = len(leaver_ids)
    leavers_train_test_threshold = int(leavers_size_population * 0.70)

    stayers_size_population = len(stayers_ids)
    stayers_train_test_threshold = int(stayers_size_population * 0.70)


    leavers_train_ids,leavers_test_ids = leaver_ids[:leavers_train_test_threshold], leaver_ids[leavers_train_test_threshold:]
    stayers_train_ids,stayers_test_ids = stayers_ids[:stayers_train_test_threshold], leaver_ids[stayers_train_test_threshold:]

    AbstractDataPipe.trainDframe = AbstractDataPipe.Dframe.filter(f.col('employee_id').isin(leavers_train_ids + stayers_train_ids))
    AbstractDataPipe.testDframe  = AbstractDataPipe.Dframe.filter(f.col('employee_id').isin(leavers_test_ids + stayers_test_ids))

class DataTypeConvert(ABC):
    """
    DataTypeConvert converts pyspark.DataFrame
    colunms to a specified datatype
    """
    @abstractmethod
    def apply(self,Dframe,params:Dictionary):
        pass

class toDouble(DataTypeConvert):
    def apply(self,Dframe,params:Dictionary):
        Dframe =  Dframe.select(*(f.col(c).cast("double").alias(c)
                            if c in params["double"].values() else f.col(c) for c in AbstractDataPipe.Dframe.columns))
