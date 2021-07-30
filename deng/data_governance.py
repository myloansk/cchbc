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
    """
    DataTest class initiates an Facade object(data_test) that provides an interface
    to the DataCheck class that contains methods that run a number of descriptive stats
    on the inhereted pyspark.DataFrame by AbstractDataPipe
    """
    def pipe(self):

      data_test = DataCheckFacade(AbstractDataPipe.Dframe)
      data_test.check_buffer()
      AbstractDataPipe.step  = AbstractDataPipe.step + 1
      return  super().pipe()

  class DataTransform(AbstractDataPipe):
    """
    DataTransform class converts columns pyspark.DataFrame to double.
    Both the pyspark.DataFrame and the columns to be converted are provided through inheritance by parent class AbstractDataPipe
    """
    def pipe(self):

        AbstractDataPipe.Dframe =  AbstractDataPipe.Dframe.select(*(f.col(c).cast("double").alias(c)
                            if c in AbstractDataPipe.pipeline_param_dictionary[3] else f.col(c) for c in AbstractDataPipe.Dframe.columns))
        AbstractDataPipe.step  = AbstractDataPipe.step + 1
        return  super().pipe()

 class DataMissingImpute(AbstractDataPipe):
    """
    DataMissingImpute class initiates a facade object(imputer) that delegates
    the imputation of missing values of pyspark.DataFrame to appropriate methods of MissingImputerHandler
    class. The imputation strategy followed on each column along with pyspark.DataFrame is provided through inheritance by AbstractDataPipe
    """
    def pipe(self):
        imputer = ImputerFacade(ZeroImputeHandler(),UnknownImputeHandler(),MeanImputeHandler(),
                                FfillImputeHandler(),NfillImputeHandler(),BucketImputeHandler())
        for mtd in set(self.IMPUTE_DICT.values()):

            col_lst = [k for k,v in self.IMPUTE_DICT.items() if  (v==mtd) and (k in AbstractDataPipe.Dframe.columns)]

            AbstractDataPipe.Dframe = imputer.impute_operation(mtd,AbstractDataPipe.Dframe,col_lst)

        amount_missing_df = AbstractDataPipe.Dframe.select([(f.count(f.when(f.isnan(c) | f.col(c).isNull(), c))/f.count(f.lit(1))).alias(c)
                                                            for c in  AbstractDataPipe.Dframe.columns])
        display(amount_missing_df)
        AbstractDataPipe.step  = AbstractDataPipe.step + 1
        return  super().pipe()

class DataOneHotEncode(AbstractDataPipe):
  def pipe(self):

    AbstractDataPipe.Dframe = AbstractDataPipe.Dframe.drop(*AbstractDataPipe.pipeline_param_dictionary[7])
    continuous_columns = [c[0] for c in AbstractDataPipe.Dframe.dtypes
                       if c[1] in ['integer', 'double', 'bigint','tinyint', 'float'] if c[0] not in ['employee_id','year_month','label','year']+LABEL_COLUMNS]
    categorical_columns = [c[0] for c in AbstractDataPipe.Dframe.dtypes if c[1] in ['string'] if c[0] not in  ['employee_id','year_month','label','year']+LABEL_COLUMNS]
    for cat_col in categorical_columns:
        categorical_column_values = AbstractDataPipe.Dframe.select(cat_col).distinct().rdd.flatMap(lambda x:x).collect()
        expr = [f.when(f.col(cat_col) == cat,1).otherwise(0)\
                                         .alias(str(cat_col+'='+cat)) for cat in categorical_column_values]
        AbstractDataPipe.Dframe = AbstractDataPipe.Dframe.select( expr + AbstractDataPipe.Dframe.columns )\
                                  .drop(cat_col)
    AbstractDataPipe.step  = AbstractDataPipe.step + 1
    return  super().pipe()
