import numpy as np
import pandas as pd
import swifter

class DataCleaner():

    def remove_columns(self, df, columns=None):
        '''
        Removes selected columns from selected dataframe
        :param df: Dataframe which was removed selected columns
        :param columns: The columns that were removed from selected Dataframe
        :return: dataframe: Dataframe that we removed unnecessary columns
        '''
        if columns != None:
            if type(df) == type(pd.DataFrame()):
                df.drop(columns, axis=1, inplace=True)

                return df
            else:
                raise TypeError('df parameter must be Pandas Dataframe')
        else:
            return "Columns name are empty!!"

    def remove_rows_by_condition(self,df, dict):
        '''
        Filter selected DataFrame by given dictionary key('columnName') and value('condition')
        :param df: Pandas Dataframe which will be filtered
        :param dict: Dictionary which has column name and condition
        :return: New filtered dataframe
        '''
        if dict != None:
            if type(df) == type(pd.DataFrame()):
                new_df = df.copy()
                for column, condition in dict.items():
                    f = lambda x: eval(condition)
                    new_df = new_df.loc[new_df[column].swifter.apply(f)]

                return  new_df

    def remove_outliers(self, df, low,high):
        low = low
        high = high
        quant_df = df.quantile([low, high])
        for name in list(df.columns):
            if pd.api.types.is_numeric_dtype(df[name]):
                df = df[(df[name] > quant_df.loc[low, name])
                        & (df[name] < quant_df.loc[high, name])]
        return df

    #TODO remove unique columns
    def remove_unique_columns(self,df):
        pass

    #TODO remove duplicate columns
    def remove_duplicate_columns(self, df):
        pass

