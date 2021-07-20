from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Optional,List

class NullHandler(ABC):

    @abstractmethod
    def set_next(self,null_handler:NullHandler) -> NullHandler:
        pass
    @abstractmethod
    def handle(self,impute_method,sdf,colList):
        pass
class AbstractNullHandler(NullHandler):

    _next_handler: NullHandler = None

    def set_next(self,null_handler:NullHandler) -> NullHandler:
        self._next_handler = null_handler
        return null_handler

    @abstractmethod
    def handle(self,impute_method,sdf,colList):
        if self._next_handler:
            return self._next_handler.handle(impute_method,sdf,colList)
        return sdf


class ZeroImputeHandler(AbstractNullHandler):
    def handle(self,impute_method,sdf,colList):
        if impute_method == '0':
                for colName in colList:
                    sdf = sdf.withColumn(colName,f.when(f.col(colName).isNull(),f.lit(0)).otherwise(f.col(colName)))
                return sdf
            else:
                return  super().handle(impute_method,sdf,colList)
class MeanImputeHandler(AbstractNullHandler):
    def handle(self,impute_method,sdf,colList):
        if impute_method == 'avg':

                w = Window().partitionBy('employee_id').orderBy(f.col('year_month').cast("integer").desc())
                w1 = Window().partitionBy('employee_id').orderBy(f.col('year_month').cast("integer").desc()).rowsBetween(0,3)
                w2 = Window().partitionBy('employee_id').orderBy(f.col('year_month').cast("integer").desc()).rowsBetween(0,6)
                w3 = Window().orderBy(f.col('year_month').cast("integer").desc()).rowsBetween(0,3)
                w4 = Window().orderBy(f.col('year_month').cast("integer").desc()).rowsBetween(0,6)
                w5 = Window().orderBy(f.col('year_month').cast("integer").desc())
                for colName in colList:
                    sdf = sdf.withColumn('rolling_mean_per_bd_1Mon'+ colName,f.first(f.col(colName)).over(w))\
                                               .withColumn('rolling_mean_per_bd_3Mon' + colName,f.mean(f.col(colName)).over(w1))\
                                               .withColumn('rolling_mean_per_bd_6Mon' + colName,f.mean(f.col(colName)).over(w2))\
                                               .withColumn('rolling_mean_total_3Mon' + colName,f.mean(f.col(colName)).over(w3))\
                                               .withColumn('rolling_mean_total_6Mon' + colName,f.mean(f.col(colName)).over(w4))\
                                               .withColumn('rolling_mean_total' + colName,f.mean(f.col(colName)).over(w5))\
                                               .withColumn("rolling_mean_impute" + colName,
                                                           f.coalesce(f.col("rolling_mean_per_bd_1Mon" + colName),
                                                                       f.col("rolling_mean_per_bd_3Mon" + colName),
                                                                       f.col("rolling_mean_per_bd_6Mon"+ colName),
                                                                       f.col("rolling_mean_total_3Mon" + colName),
                                                                       f.col("rolling_mean_total_6Mon" + colName),
                                                                       f.col("rolling_mean_total" + colName)).cast("string"))


                for colName in colList:
                   sdf = sdf.withColumn(colName,
                                        f.coalesce(f.col(colName),f.col("rolling_mean_impute" + colName)).cast('float'))
                return sdf
            else:
                return super().handle(impute_method,sdf,colList)

class UnknownImputeHandler(AbstractNullHandler):
    def handle(self,impute_method,sdf,colList):
        if impute_method == 'unknown':
                for colName in colList:
                   sdf = sdf.withColumn(colName,f.when((f.col(colName).isNull()) | (f.col(colName) == 'null')
                                               ,f.lit('Unknown')).otherwise(f.col(colName)))
                return sdf
            else:
                return  super().handle(impute_method,sdf,colList)

class FfillImputeHandler(AbstractNullHandler):
    def handle(self,impute_method,sdf,colList):
        pass

class NfillImputeHandler(AbstractNullHandler):
    def handle(self,impute_method,sdf,colList):
        pass
class BucketImputeHandler(AbstractNullHandler):
    def handle(self,impute_method,sdf,colList):
        pass
