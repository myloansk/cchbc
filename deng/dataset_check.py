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
        return self.Dframe.count()
    def cnt_column(self):
        return len(self.Dframe.columns)

    def data_types(self):pass
        self.Dframe.printScema()

    def summary(self):pass

    def diff_sdf(self):pass

    def null_percentage_per_column(self):
         amount_missing_df = self.Dframe\
                                 .select([(f.count(f.when(f.isnan(c) | f.col(c).isNull(), c))/f.count(f.lit(1))).alias(c)
                                          for c in  self.Dframe.columns])
    def show_five(self):
        self.Dframe.take(5)
