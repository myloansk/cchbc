from __future__ import annotations
from abc import ABC, abstractmethod
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import DataFrame

class Utilities(ABC):
    """
    The Command interface declares a method for executing a command.
    """

    @abstractmethod
    def execute(self) -> None:
        pass

class saveResults(Utilities):
   """
   Some commands can implement simple operations on their own.
   """

   def __init__(self, df: DataFrame, location: str) -> None:
       self._df= df
       self._location = location

   def execute(self) -> None
     # Save to temp location
     temp_location = self._location.rsplit('.',1)[0]+'.temp'
     self._df   = self._df.coalesce(24).write.mode('overwrite').format("parquet").saveAsTable(temp_location)

     # Drop existing table
     spark.sql(f'DROP TABLE IF EXISTS {location}')

     # Load temp dataframe and save to location
     self._df =  spark.sql(f'select * from {temp_location}')
     self._df =  self._df.coalesce(24).write.mode('overwrite').format("parquet").saveAsTable(location)

     # Drop temp table
     spark.sql(f'DROP TABLE IF EXISTS {temp_location}')

class dataCheck(Utilities):
       """
       Some commands can implement simple operations on their own.
       """

       def __init__(self, df: DataFrame, location: str) -> None:
           self._df= df
           self._location = location

       def execute(self) -> None
         # Save to temp location
         self._df = spark.sql(f'select * from {location}')
         print('')

         # Null check
         nullDf = elf._df.select([f.count(f.when(f.isnan(c) | f.col(c).isNull(), c)).alias(c) for c in self._df.columns])
         nullDict = nullDf.collect()[0].asDict()

         print('Columns with nulls:')
         nullDf.select(*[c for c in nullDict if nullDict[c]>0]).show()

         # MT/FT split count
         if 'MT_FT' in self._df.columns:
           print('MT/FT outlet count:')
           self._df.groupBy('MT_FT').count().show()
