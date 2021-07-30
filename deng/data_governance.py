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


class DataExtract(AbstractDataPipe):
    """
    DataExtract extracts data from hive a table and stored them to a pyspark.DataFrame
    The query used for the data extraction and pyspark.DataFrame to which data are going to be store
    are inheted by parent class AbstractDataPipe
    Finally, it returns control to parent class method pipe once data extracted trainDframe
    and pyspark.DataFrame is populated with them 
    """
    def pipe(self):
        AbstractDataPipe.step = 1
        query = AbstractDataPipe.pipeline_param_dictionary[1][1].format(cc=self.country,db=TEMP_DB)
        AbstractDataPipe.Dframe = spark.sql(query)
        AbstractDataPipe.step  = AbstractDataPipe.step + 1

        return  super().pipe()

class DataTest(AbstractDataPipe):
    def pipe(self):

      data_test = DataCheckFacade(AbstractDataPipe.Dframe)
      data_test.check_buffer()
      AbstractDataPipe.step  = AbstractDataPipe.step + 1
      return  super().pipe()
