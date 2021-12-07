from __future__ import annotations
from abc import ABC, abstractmethod
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import DataFrame

class Utilities(ABC):
    """
    The Utilities interface declares a method(execute)
    for executing a commands defined below.
    """

    @abstractmethod
    def execute(self) -> None:
        pass

class saveResults(Utilities):
    """
    saveResults class saves pyspark.Dataframe in parquet format
    to a temporary location defined by location passed as an argument by client
    followed by suffix temp before it moved to the permanent location that user
    provided at initiliation of object
    Args:
     df:pyspark.Dataframe provided by client to be stored in specified _location
     location:string file directory in which data in pyspark.Dataframe will be store
    """
    def __init__(self, df: DataFrame, location: str) -> None:
        self._df= df
        self._location = location

    def execute(self) -> None:
        temp_location = self._location.rsplit('.',1)[0]+'.temp'
        self._df   = self._df.coalesce(24).write.mode('overwrite').format("parquet").saveAsTable(temp_location)
        # Drop existing table
        spark.sql(f'DROP TABLE IF EXISTS {location}')
        self._df =  spark.sql(f'select * from {temp_location}')
        self._df =  self._df.coalesce(24).write.mode('overwrite').format("parquet").saveAsTable(location)

        # Drop temp table
        spark.sql(f'DROP TABLE IF EXISTS {temp_location}')

class dataCheck(Utilities):
    """
    dataCheck class checks pyspark.Dataframe columns for nulls and return those
    columns which have nulls 
    Args:
     df:pyspark.Dataframe provided by client to be stored in specified _location
     location:string file directory in which data in pyspark.Dataframe will be store
    """
    def __init__(self, df: DataFrame, location: str) -> None:
        self._df= df
        self._location = location

    def execute(self) -> None:
        self._df = spark.sql(f'select * from {location}')
        print('')

        # Null check
        nullDf = self._df.select([f.count(f.when(f.isnan(c) | f.col(c).isNull(), c)).alias(c) for c in self._df.columns])
        nullDict = nullDf.collect()[0].asDict()

        print('Columns with nulls:')
        nullDf.select(*[c for c in nullDict if nullDict[c]>0]).show()

        # MT/FT split count
        if 'MT_FT' in self._df.columns:
            print('MT/FT outlet count:')
            self._df.groupBy('MT_FT').count().show()
