from abc import ABC, abstractmethod
from __future__ import annotations
from collections import Counter
from pyspark.sql import DataFrame
from pyspark.mllib.stat import Statistics
from typing import Any, Optional,List
from utils.correlation_functiions import compute_associations
import inspect
import pyspark.sql.functions as f
import pyspark.sql.types as t

class Correlation(ABC):
    """
    The Correlation interface declares a method(execute)
    for executing a commands defined below.
    """
    def __init__(self, replace:str='replace',
                 drop:str='drop',drop_samples:str='drop_samples',drop_features:str='drop_features',
                 skip:str='skip',default_replace_value:float=0.0,data_types_lst:List[str]=[],
                 exclude_lst:List[str]=[])->None:

        self._corrDf  = None
        self._replace = replace
        self._drop = drop
        self._drop_samples =  drop_samples
        self._drop_features = drop_features
        self._skip = skip
        self._default_replace_value = default_replace_value
        self._data_types_lst = data_types_lst
        self._exclude_lst = exclude_lst

    @abstractmethod
    def calculate_correlations(self):
        pass

    @abstractmethod
    def remove_correlated_features(self):
        pass


@delegates()
class correlationsDelegatesCalculationToPypsparkApi(Correlation):
    def __init__(self,**kwargs)->None:super().__init__(self,**kwargs)

    def calculate_correlations(self,inputDf:DataFrame,col_list:List):
        availableCols = [c[0] for c in inputDf.dtypes
                         if c[1] in self._data_types_lst if c[0] not in self._exclude_lst]

        priority_list = list(availableCols)
        features = inputDf.select(availableCols).rdd.map(lambda row: row[:])

        return pd.DataFrame(Statistics.corr(features), index=priority_list,
                                           columns=priority_list)

   def remove_correlated_features(self, inputDf:DataFrame, col_lst:List, ubound:float = 0.85)->DataFrame:
       corr_m = self.calculate_correlations(inputDf,col_lst)
       upper =  corr_m.where(np.triu(corr_m, k=1).astype(np.bool))
       # Only consider correlation over ubound
       upper[upper < ubound] = np.nan
       # For each column, find the highest correlation pair and the sort results
       highest_corr_pairs = upper.loc[:, upper.max().sort_values(ascending=False).index].idxmax().dropna()
       sorted_corr_pairs = list(zip(highest_corr_pairs, highest_corr_pairs.index))

       col_correlated = [sorted(i, key=priority_list.index, reverse=True)[0] for i in sorted_corr_pairs]

       outputDf = inputDf.drop(*set(col_correlated))

       # Print columns that were removed
       print(*('Removing column: {} '.format(c) for c in set(col_correlated)), sep='\n')
       return outputDf         

@delegates()
class correlationsDelegatesCalcToPythonLibs(Correlation):
    def __init__(self, **kwargs)->None:super().__init__(self, **kwargs)

    def calculate_correlations(self,inputDf:DataFrame,col_list:List):
        return spark.createDataFrame(compute_associations(inputDf[col_lst],nominal_columns='auto',
                                                                  numerical_columns=None,mark_columns=False,nom_nom_assoc='cramer',
                                                                  num_num_assoc='pearson',bias_correction=True,
                                                                  nan_strategy=self._drop_samples,nan_replace_value=self._default_replace_value,
                                                                  clustering=False
                                                                 ))
