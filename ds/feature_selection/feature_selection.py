from __future__ import annotations
from abc import ABC, abstractmethod
from typing import List

import pandas as pd
import numpy as np
import scipy.stats as stats
from scipy.stats import chi2_contingency

class IFeatureSelection(ABC):
    """
    The IFeatureSelection interface declares operations common to all supported versions
    of some algorithm.

    The Context uses this interface to call the algorithm defined by Concrete
    Strategies.
    """

    @abstractmethod
    def select_fetures(self, *args):
        pass

class ChiSquare(IFeatureSelection):
    """
    ChiSquare implements an algorithm of feature selection while following the base IFeatureSelection
    interface. The interface makes them interchangeable in the Context.


    :param IFeatureSelection: interface that declares operations common to all concreate imple
    :type IFeatureSelection: class
    """
    def __init__(self, dataframe):
        self.df = dataframe
        self.p = None #P-Value
        self.chi2 = None #Chi Test Statistic
        self.dof = None
        
        self.dfTabular = None
        self.dfExpected = None
        self.importantColsLst = []
        
    def _print_chisquare_result(self, colX, alpha):
        result = ""
        if self.p<alpha:
            self.importantColsLst.append(colX)
            result="{0} is IMPORTANT for Prediction".format(colX)
        else:
            result="{0} is NOT an important predictor. (Discard {0} from model)".format(colX)
        print(result)
        
    def select_fetures(self,colX,colY, alpha=0.03):
        X = self.df[colX].astype(str)
        Y = self.df[colY].astype(str)
        
        self.dfObserved = pd.crosstab(Y,X) 
        chi2, p, dof, expected = stats.chi2_contingency(self.dfObserved.values)
        self.p = p
        self.chi2 = chi2
        self.dof = dof 
        
        self.dfExpected = pd.DataFrame(expected, columns=self.dfObserved.columns, index = self.dfObserved.index)
        
        self._print_chisquare_result(colX, alpha)