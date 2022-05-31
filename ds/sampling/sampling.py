from abc import ABC, abstractclassmethod

from pandas import DataFrame
from typing import Dict, List, Tuple 
class ISampling(ABC):
    """
    The ISampling interface declares operations(take_sample) common to all supported versions

    The Context uses this interface to call the algorithm defined by Concrete
    Implementation of ISampling to take a sample out of data.
    """
    @abstractclassmethod
    def take_sample(self, sparkDf:DataFrame, label:str,join_on:str,
                    ratio:str = 0.7, seed=42)->DataFrame:pass 

