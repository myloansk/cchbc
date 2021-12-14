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
    def __init__(self,inputDf:DataFrame, col_list:List, **kwargs)->None:
        super().__init_(self,inputDf,col_lst,data_types_lst=['int', 'double', 'bigint', 'float'],exclude_lst=[])

    def calculate_correlations(self)->DataFrame:
        availableCols = [c[0] for c in self._inputDf.dtypes
                         if c[1] in self.data_types_lst if c[0] not in self.exclude_lst]

        priority_list = list(availableCols)
        features = inputDf.select(availableCols).rdd.map(lambda row: row[:])
        self._corrDf = pd.DataFrame(Statistics.corr(features), index=priority_list,
                                    columns=priority_list)

        return self._corrDf

@delegates()
class correlationsDelegatesCalcToPythonLibs(Correlation):
    def __init__(self,inputDf:DataFrame,col_list:List, **kwargs)->None:
        super().__init_(self,inputDf,col_lst, replace:str='replace',drop:str='drop',drop_samples:str='drop_samples',
         drop_features:str='drop_features',skip:str='skip',default_replace_value:float=0.0)

    def calculate_correlations(self)->DataFrame:
        self._corrDf = compute_associations(self._inputDf[self._col_lst],nominal_columns='auto',
                                            numerical_columns=None,mark_columns=False,nom_nom_assoc='cramer',
                                            num_num_assoc='pearson,bias_correction=True,
                                            nan_strategy=self.drop_samples,nan_replace_value=self.default_replace_value,
                                            clustering=False
                                           )
