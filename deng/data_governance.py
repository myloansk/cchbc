from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Optional,List


class DataPipe(ABC):

    @abstractmethod
    def set_next_stage(self,null_handler:NullHandler) -> NullHandler:
        pass
    @abstractmethod
    def pipe(self,impute_method,sdf,colList):
        pass

class AbstractDataPipe(DataPipe):
  """
  """
  _next_handler: DataPipe = None
  def __init__(self,country,name):
     self.init_ts = datetime.datetime.now().strftime("%Y%m%d_ %H%M%S")
     self.name  = name + '_' + country + '_' + datetime.datetime.now().strftime("%Y%m%d_ %H%M%S")
     self.country = country
     self.Dframe = None

  def set_next_stage(self,null_handler:DataPipe) -> DataPipe:
      self._next_handler = null_handler

      return null_handler

  @abstractmethod
  def pipe(self,impute_method,sdf,colList):
    if self._next_handler:
        return self._next_handler.handle(impute_method,sdf,colList)
    return sdf

class DataIngest(AbstractDataPipe):
    def pipe(self):
        self.Dframe = spark.sql("SELECT * FROM {cc}_db_app_bdto_de_l3.tx_bdto_features_tmp".format(cc=self.country))
        return  super().handle(impute_method,sdf,colList)
