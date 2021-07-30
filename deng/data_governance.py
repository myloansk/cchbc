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
  pipeline_param_dictionary = {  1:[EXTRACT_RAW_DATA_QUERY,EXTRACT_MODEL_INPUT_DATA_QUERY],
                                 3:CONVERT_NUM,
                                 4:PERIOD,
                                 5:[CORRELATION_EXCLUDE_LIST,UBOUND],
                                 6:CURATED_TABLE,
                                 7:ONE_HOT_ENCODED_EXCLUDED_COLUMNS,
                                 8:MODEL_EXCLUDED_COLUMNS
                              }

  def __init__(self,country,name):
      self.init_ts = datetime.datetime.now().strftime("%Y%m%d_ %H%M%S")
      self.name  = name + '_' + country + '_' + datetime.datetime.now().strftime("%Y%m%d_ %H%M%S")
      self.country = country
      self.Dframe = None
      self.model_input_columns = None
      self.trainDframe = None
      self.testDframe = None
      self.step  = None

  def set_next_stage(self,null_handler:DataPipe) -> DataPipe:
      self._next_handler = null_handler
      return null_handler

  @abstractmethod
  def pipe(self):
    if self._next_handler:
        return self._next_handler.pipe()
    return self.Dframe
