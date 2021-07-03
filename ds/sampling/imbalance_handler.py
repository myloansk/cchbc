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
        return self.sdf

class ImbalanceHandlerReceiver(object):
    """
    """
    def __init__(self,sample):
        self.sampler = sampler
        self.num_cols = None
        self.cat_cols = None
        self.target = None
        self.index_suffix = "_index"
        self.seed = None
        self.bucketLength = None
        self.k = None
        self.multiplier = None

    def get_params(self,sdf,num_cols,cat_cols,target_col,seed=48,bucketLength=100,k=4,multiplier=10):
                self.num_cols = num_cols
                self.cat_cols = cat_cols
                self.target = target_col
                self.index_suffix = "_index"
                self.seed = seed
                self.bucketLength = bucketLength
                self.k = k
                self.multiplier = multiplier
    def to_array(col):
        def to_array_(v):
            return v.toArray().tolist()
        return udf(to_array_, t.ArrayType(t.DoubleType())).asNondeterministic()(col)


    @f.udf(returnType=VectorUDT())
    def subtract_vector_udf(arr):
        # Must decorate func as udf to ensure that its callback form is the arg to df iterator construct
        a = arr[0]
        b = arr[1]
        if isinstance(a, SparseVector):
            a = a.toArray()
        if isinstance(b, SparseVector):
            b = b.toArray()
        array_ = a - b
        return random.uniform(0, 1) * Vectors.dense(array_)

    @f.udf(returnType=VectorUDT())
    def add_vector_udf(arr):
        # Must decorate func as udf to ensure that its callback form is the arg to df iterator construct
        a = arr[0]
        b = arr[1]
        if isinstance(a, SparseVector):
            a = a.toArray()
        if isinstance(b, SparseVector):
            b = b.toArray()
        array_ = a + b
        return Vectors.dense(array_)

    def pre_smote_df_process(self,df):
        '''
        string indexer and vector assembler
        inputs:
        * df: spark df, original
        * num_cols: numerical cols to be assembled
        * cat_cols: categorical cols to be stringindexed
        * target_col: prediction target
        * index_suffix: will be the suffix after string indexing
        output:
        * vectorized: spark df, after stringindex and vector assemble, ready for smote
        '''
        if(df.select(self.target_col).distinct().count() != 2):
            raise ValueError("Target col must have exactly 2 classes")

        if target_col in sefl.num_cols:
            self.num_cols.remove(self.target_col)

        # only assembled numeric columns into features
        assembler = VectorAssembler(inputCols = self.num_cols, outputCol = 'features')
        # index the string cols, except possibly for the label col
        assemble_stages = [StringIndexer(inputCol=column, outputCol=column+self.index_suffix).fit(df) for column in list(set(self.cat_cols)-set([self.target_col]))]
        # add the stage of numerical vector assembler
        assemble_stages.append(assembler)
        pipeline = Pipeline(stages=assemble_stages)
        pos_vectorized = pipeline.fit(df).transform(df)

        # drop original num cols and cat cols
        drop_cols = self.num_cols+self.cat_cols

        keep_cols = [a for a in pos_vectorized.columns if a not in drop_cols]

        vectorized = pos_vectorized.select(*keep_cols).withColumn('label',pos_vectorized[target_col]).drop(target_col)

        return vectorized

    #conf = config.conf_template.Struct("smote_config")
    #conf.seed = 48
    #conf.bucketLength = 100
    #conf.k = 4
    #conf.multiplier = 3

    def smote(self,vectorized_sdf):
        '''
        contains logic to perform smote oversampling, given a spark df with 2 classes
        inputs:
        * vectorized_sdf: cat cols are already stringindexed, num cols are assembled into 'features' vector
          df target col should be 'label'
        * smote_config: config obj containing smote parameters
        output:
        * oversampled_df: spark df after smote oversampling
        '''
        dataInput_min = vectorized_sdf[vectorized_sdf['label'] == 1]
        dataInput_maj = vectorized_sdf[vectorized_sdf['label'] == 0]

        # LSH, bucketed random projection
        brp = BucketedRandomProjectionLSH(inputCol="features", outputCol="hashes",
                                          seed=self.seed, sefl.bucketLength=self.bucketLength)
        # smote only applies on existing minority instances
        model = brp.fit(dataInput_min)
        model.transform(dataInput_min)

        # here distance is calculated from brp's param inputCol
        self_join_w_distance = model.approxSimilarityJoin(dataInput_min, dataInput_min, float("inf"), distCol="EuclideanDistance")

        # remove self-comparison (distance 0)
        self_join_w_distance = self_join_w_distance.filter(self_join_w_distance.EuclideanDistance > 0)

        over_original_rows = Window.partitionBy("datasetA").orderBy("EuclideanDistance")

        self_similarity_df = self_join_w_distance.withColumn("r_num", f.row_number().over(over_original_rows))

        self_similarity_df_selected = self_similarity_df.filter(self_similarity_df.r_num <= k)

        over_original_rows_no_order = Window.partitionBy('datasetA')

        # list to store batches of synthetic data
        res = []

        # two udf for vector add and subtract, subtraction include a random factor [0,1]
        #subtract_vector_udf = f.udf(subtract_vector_fn, VectorUDT())
        #add_vector_udf = f.udf(add_vector_fn, VectorUDT())


        # retain original columns
        original_cols = dataInput_min.columns

        for i in range(self.multiplier):
            print("generating batch %s of synthetic instances"%i)
            # logic to randomly select neighbour: pick the largest random number generated row as the neighbour
            df_random_sel = self_similarity_df_selected.withColumn("rand", f.rand()).withColumn('max_rand', f.max('rand').over(over_original_rows_no_order))\
                                .where(f.col('rand') == f.col('max_rand')).drop(*['max_rand','rand','r_num'])
            # create synthetic feature numerical part
            df_vec_diff = df_random_sel.select('*', subtract_vector_udf(f.array('datasetA.features', 'datasetB.features')).alias('vec_diff'))
            df_vec_modified = df_vec_diff.select('*', add_vector_udf(f.array('datasetB.features', 'vec_diff')).alias('features'))

            # for categorical cols, either pick original or the neighbour's cat values
            for c in original_cols:
                # randomly select neighbour or original data
                col_sub = random.choice(['datasetA','datasetB'])
                val = "{0}.{1}".format(col_sub,c)
                if c != 'features':
                    # do not unpack original numerical features
                    df_vec_modified = df_vec_modified.withColumn(c,f.col(val))

            # this df_vec_modified is the synthetic minority instances,
            df_vec_modified = df_vec_modified.drop(*['datasetA','datasetB','vec_diff','EuclideanDistance'])


            res.append(df_vec_modified)

        dfunion = reduce(DataFrame.unionAll, res)


        dfunion = dfunion.withColumn('is_replicant',f.lit(1))
        vectorized_sdf = vectorized_sdf.withColumn('is_replicant',f.lit(0))
        unionCol = dfunion.columns
        # union synthetic instances with original full (both minority and majority) df
        oversampled_df = dfunion.union(vectorized_sdf.select(dfunion.columns))

        return oversampled_df

    def restore_smoted_df(self,smoted_df,vectorized_col):
        '''
        restore smoted df to original type
        with original num_cols names
        and stringIndexed cat cols, suffix _index
        depending on to_array udf to unpack vectorized col
        * vectorized_col: str, col that is vectorized
        '''
        # based on the assumption that vectorization is by the list sequence of num_cols
        # to array first
        smoted_df = smoted_df.withColumn("array_num_cols", to_array(f.col(vectorized_col)))
        # restore all num_cols
        for i in range(len(self.num_cols)):
            smoted_df = smoted_df.withColumn(num_cols[i], f.col("array_num_cols")[i])

        drop_cols = [vectorized_col,'array_num_cols']
        return smoted_df.drop(*drop_cols)

    def get_samples(self):
        pass





class OverSampling(ImbalanceHandler):
    """
    """
    def __init__(self,receiver:ImbalanceHandlerReceiver):
        self._receiver

    def get_samples(self):
        self._receiver.get_samples()

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
