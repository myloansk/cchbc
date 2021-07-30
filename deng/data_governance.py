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
    """
    DataOneHotEncode class create indicator varibles for each combination
    of categorical feature and its respective value range in a pyspark.DataFrame
    """
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

class DataSlice(AbstractDataPipe):
    """
    DataSlice class updates pyspark.DataFrame by keeping rows corresponding to certain periods(year_month)
    The period of interest and pyspark.DataFrame are inheted by parent class AbstractDataPipe
    """
  def pipe(self):

      AbstractDataPipe.Dframe = AbstractDataPipe.Dframe.filter(f.col('year_month').isin(AbstractDataPipe.pipeline_param_dictionary[4]) ).drop('year')
      AbstractDataPipe.step  = AbstractDataPipe.step + 1
      return  super().pipe()

  class DataCorrelation(AbstractDataPipe):
    """
    DataCorrelation removes highly correlated features in the pyspark.DataFame.
    In particular ,it removes one feature  from a pair of features whose correlation exceeds a threshold
    The threshold(UBOUND) and pyspark.DataFrame are inheted by parent class AbstractDataPipe
    """
  def pipe(self):

      correlation_exclude_list = AbstractDataPipe.pipeline_param_dictionary[5][0]
      upper_bound              = AbstractDataPipe.pipeline_param_dictionary[5][1]
      availableCols = [c[0] for c in  AbstractDataPipe.Dframe.dtypes if c[1] in ['int', 'double', 'bigint', 'float'] if c[0] not in correlation_exclude_list]
      priority_list = list(availableCols)
      features =  AbstractDataPipe.Dframe.select(availableCols).rdd.map(lambda row: row[:])
      corr_m = pd.DataFrame(Statistics.corr(features), index=priority_list, columns=priority_list)

      upper = corr_m.where(np.triu(corr_m, k=1).astype(np.bool))
      upper[upper < upper_bound] = np.nan # Only consider correlation over ubound

      highest_corr_pairs = upper.loc[:, upper.max().sort_values(ascending=False).index].idxmax().dropna() # For each column, find the highest correlation pair and the sort results

      sorted_corr_pairs = list(zip(highest_corr_pairs, highest_corr_pairs.index))
      for fk,fv in dict(zip(highest_corr_pairs, highest_corr_pairs.index)).items():
          print('correlated pairs({key}->{value})'.format(key=fk,value=fv))
          col_correlated = [sorted(i, key=priority_list.index, reverse=True)[0] for i in sorted_corr_pairs]
          # Drop columns
          AbstractDataPipe.Dframe =  AbstractDataPipe.Dframe.drop(*set(col_correlated))
          # Print columns that were removed
      print(*('Removing column: {} '.format(c) for c in set(col_correlated)), sep='\n')
      AbstractDataPipe.step  = AbstractDataPipe.step + 1
      return super().pipe()

  class DataSave(AbstractDataPipe):
    """
    DataSave save pyspark.DataFrame to specific table in hive
    whose name(database name + tablename) is proivided through inheritance by parent class AbstractDataPipe """
  def pipe(self):
      curated_table = AbstractDataPipe.pipeline_param_dictionary[6].format(db=TEMP_DB,cc=self.country)

      AbstractDataPipe.Dframe.coalesce(24)\
                      .write\
                      .mode('overwrite')\
                      .format("parquet")\
                      .saveAsTable(curated_table)
      AbstractDataPipe.step  = AbstractDataPipe.step + 1
      return super().pipe()

class DataSplit(AbstractDataPipe):
    """
    DataSplit class split pyspark.DataFrame to train/test  pyspark.DataFRames  in 70/30 ratio
    on BD wise level in order employees_id found in one set would not be present in the other to avoid any model leakage

    """
  def pipe(self):
    column_names =  AbstractDataPipe.Dframe.columns
    AbstractDataPipe.model_input_columns =  [item for item in colNames if item not in AbstractDataPipe.pipeline_param_dictionary[8] and '=Unknown' not in item]

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

    return super().pipe()

class DataOversampling(AbstractDataPipe):
  def pipe(self):
    continuousCols = [c[0] for c in AbstractDataPipe.trainDframe.dtypes
                       if c[1] in ['integer', 'double', 'bigint','tinyint', 'float'] if c[0] not in ['employee_id','year_month','label','year'] + LABEL_COLUMNS]
    categoricalCols = [c[0] for c in AbstractDataPipe.trainDframe.dtypes
                       if c[1] in ['string'] if c[0] not in  ['employee_id','year_month','label','year'] + LABEL_COLUMNS]
    AbstractDataPipe.trainDframe =AbstractDataPipe.trainDframe.withColumn('updated_voluntary_leave_6Mafter',
                                          f.when(f.col('voluntary_leave_6Mafter') != 9999,
                                          f.col('voluntary_leave_6Mafter')).otherwise(f.lit(0)) )

    AbstractDataPipe.testDframe =AbstractDataPipe.testDframe.withColumn('label',
                                                                        f.when(f.col('voluntary_leave_6Mafter') != 9999,
                                                                               f.col('voluntary_leave_6Mafter')).otherwise(f.lit(0)) )


    receiver = ImbalanceHandlerReceiver('get_samples')
    invoker = ImbalanceMethodInvoker()

    imb_obj = ImbalanceFacade(receiver,invoker)
    AbstractDataPipe.trainDframe = imb_obj.compute(AbstractDataPipe.trainDframe,continuousCols,categoricalCols)
    AbstractDataPipe.step  = AbstractDataPipe.step + 1
    return super().pipe()
