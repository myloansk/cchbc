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
    def __init__(self,inputDf:Dataframe, col_lst:List, replace:str='replace',
                 drop:str='drop',drop_samples:str='drop_samples',drop_features:str='drop_features',
                 skip:str='skip',default_replave_value:float=0.0,data_types_lst:List[str]=[],
                 exclude_lst:List[str]=[])->None:

        self._inputDf = inputDf
        self._col_lst = col_lst
        self._corrDf  = None
        self._replace = 'replace'
        self._drop = 'drop'
        self._drop_samples = 'drop_samples'
        self._drop_features = 'drop_features'
        self._skip = 'skip'
        self._default_replave_value = 0.0

    @abstractmethod
    def calculate_correlations(self) -> None:
        pass
    @property
    def get_input_df(self)->Dataframe:
        return self._inputDf
    @property
    def get_col_lst(self)->List:
        return self._col_lst
    @property
    def get_correlation_matrix_as_dataframe(self)->Dataframe:
        return self._corrDf

    @get_input_df.setter
    def get_input_df(self, df:Dataframe)->None:
        self._inputDf = df
    @get_col_lst.setter
    def get_col_lst(self, col_lst:List)->None:
        self._col_lst = col_lst

    def remove_correlated_features(self, ubound:float = 0.85)->DataFrame:
        upper = self._corrDf.where(np.triu(self._corrDf, k=1).astype(np.bool))
        upper[upper < ubound] = np.nan  # Only consider correlation over ubound
        highest_corr_pairs = upper.loc[:, upper.max().sort_values(ascending=False).index].idxmax().dropna()  # For each column, find the highest correlation pair and the sort results

        sorted_corr_pairs = list(zip(highest_corr_pairs, highest_corr_pairs.index))

        col_correlated = [sorted(i, key=priority_list.index, reverse=True)[0] for i in sorted_corr_pairs]

        # Drop columns
        self._inputDf = self._inputDf.drop(*set(col_correlated))

        # Print columns that were removed
        print(*('Removing column: {} '.format(c) for c in set(col_correlated)), sep='\n')

        # Return
        return self._inputDf

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
