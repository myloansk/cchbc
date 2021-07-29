class DataCheck():
    """
    class DataCheck encapsulates a number of methods that
    calculates descriptive stats on the provided dataset

    :param DataFrame sdf: pyspark DataFrame on which descriptive stats wiil be calculated
    :attr sdf:

    """
    def __init__(sdf:DataFrame)->None:
        """
        """
        self.Dframe = Dframe

    def cnt_rows(self):
        """
        @summary: Return the number of rows of pyspark.DataFrame.
        @author:  Author

        @return:  The number of rows of pyspark.DataFrame
        @rtype:   Int


        """
        return self.Dframe.count()
    def cnt_column(self):
        """
        @summary: Return the number of columns of pyspark.DataFrame.
        @author:  Author

        @return:  Return the number of columns of pyspark.DataFrame.
        @rtype:   Int
        """
        return len(self.Dframe.columns)

    def data_types(self):
        """
        Prints the pyspark.schema

        :return: None.
        :rtype: None

        """
        self.Dframe.printScema()

    def summary(self):pass

    def diff_sdf(self):pass

    def null_percentage_per_column(self):
        """
        @summary: Calculates the percentage of null/nan values of provided pyspark.DataFrame through self.
        @author:  Author

        @return:  pyspark.DataFrame having the columns of provided dataframe
                  and values the percentage of null for each column of the provided pyspark.DataFrame
        @rtype:   pyspark.DataFrame
        """
         amount_missing_df = self.Dframe\
                                 .select([(f.count(f.when(f.isnan(c) | f.col(c).isNull(), c))/f.count(f.lit(1))).alias(c)
                                          for c in  self.Dframe.columns])

    def show_five(self):
        self.Dframe.take(5)

    def agg_check(self,col,pk_col):
        """
        summary
        Provides a wrapper for checking the output of
        groupby queries

        :param (str) col: column in the pyspark.DataFrame that will be used in the groupBy clause
        :param (str) pk_col: primary key column on the data used to aggregate results.
        :return: pyspark.DataFrame grouped by a column and a
        :rtype: pyspark.DataFrame
        """
        return self.Dframe.groupBy(col).agg(f.countDistinct(pk_col))

class DataCheckFacade():
    """
    @summary: The DataCheckFacade class provides an interface to the complex logic
              invokation of DataCheck and delegates the client requests to the
              appropriate objects.
    @author:  Author

    @param    Dframe: pyspark.DataFrame`Dframe` used for calculating descriptive stats.
    @type:    type

    @param    data_check: DataCheck object used to access the calculation methods.
    @type:    type:DataCheck


    """
    def __init__(self,Dframe):
        self.data_check = DataCheck(Dframe)

    def check_buffer(self):
        print("num of rows:{nrows} num of columns:{ncol}".format(nrows=self.data_check.cnt_rows, ncol=self.data_check.cnt_columns)
        self.data_check.data_types()
        self.data_check.null_percentage_per_column()
        self.data_check.agg_check('voluntary_leave_6Mafter','employee_id')
