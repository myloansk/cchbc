from __future__ import annotations
from abc import ABC, abstractmethod
import numpy as np
import random
import re
import os
from functools import reduce

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.window import Window

from pyspark.sql import DataFrame,Row
from pyspark.ml.linalg import (SparseVector,
                               DenseVector,
                               Vectors,
                               VectorUDT)

from pyspark.ml.feature import (OneHotEncoder,
                                ChiSqSelector,
                                StringIndexer,
                                VectorAssembler,
                                BucketedRandomProjectionLSH,
                                VectorSlicer)


from pyspark.ml import Pipeline



class ImbalanceHandler(ABC):
  """
  Declares an interface for handling imbalance in Pyspark Dataframes

  [Declares a common interface to each implementation of algorithms
  for handling imbalance in pyspark Dataframe that are defined below]

  :param ABC: [description]
  :type ABC: [type]
  """
  @abstractmethod
  def get_samples(self)->None:
      pass

class DownSampling(ImbalanceHandler):
    """
    """
    def __init__(self,sdf,deised_major_to_minor_ratio, label_col, major_class_val = 0, seed = 52):
        self.sdf = sdf
        self.desired_major_to_minor_ratio
        self.label_col = label_col
        self.major_class_val = major_class_val
        self.seed = seed

    def get_samples(self):
    """
    Downsample majority class to get desired major to minor ratio, only accepts binary classification
    inputs:
        * sdf pyspark.Dataframe:  df before feature selection
        * desired_major_to_minor_ratio (int)
        * label_col: col name for label in spark df
        * major_class_val (int): The label for majority class. 0 for majority, 1 for minority by default
        * seed: for random function
    output:
        * downsampled_spark_df: the spark df after downsampling majority class.
    """
    # current distribution of 2 classes, 0 for major, 1 for minor by default
    minor_class_val = 1 - self.major_class_val
    class_count = dict(self.sdf.groupBy(col(self.label_col)).count().collect())

    # current ratio is upper bound to desired_major_to_minor_ratio
    current_maj_to_min_ratio = int(float(class_count[self.major_class_val])/float(class_count[minor_class_val]))

    # check validity in desired ratio
    if current_maj_to_min_ratio > self.desired_major_to_minor_ratio:
        # need to apply downsample
        desired_maj_samples =  self.desired_major_to_minor_ratio * class_count[minor_class_val]
        # set seed
        np.random.seed(seed)
        w = Window().orderBy(label_col)
        # index to differentiate pos/neg samples, all rows to be indexed
        self.sdf = self.sdf.withColumn("randIndex",
                                       f.when(sdf[self.label_col] == self.major_class_val, f.row_number().over(w)).otherwise(-1))

        selected_sample_index = np.random.choice(class_count[major_class_val],
                                                 desired_maj_samples, replace=False).tolist()

        sdf_sampled = self.sdf.filter(sdf['randIndex'].isin(selected_sample_index) | (self.sdf['randIndex'] < 0))\
                              .drop('randIndex')

        print("After downsampling \"{0}\": label distribution is {1}".format(label_col,sdf_sampled.groupBy(col(label_col)).count().collect()))

        return sdf_sampled
    else:
        # provided desired ratio is too large and exceed total number of majority rows
        print("Desired ratio is too large and no downsampling performed, return input dataframe.")
        return sdf

class ImbalanceHandlerReceiver():
    """

    """

class ImbalanceMethodInvoker:
    """

    """

    def __init__(self):
        self._sampler = None
        self._receiver =None

    def sample(self,sampler:ImbalanceHandler)->None:
        self._sampler = sampler

    def get_samples(self):
        self._sampler.get_samples()
