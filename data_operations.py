import feather
import dask.dataframe as dd
import pandas as pd

class DataOperations:

    def read_csv_file(self,file_path):

        '''
        Read huge csv file as dask dataframe
        :param file_path: csv file parh for read process
        :return: return read dask dataframe
        '''
        data = dd.read_csv(file_path)
        df = data.compute()
        return df

    def return_unique_data(self, list_col, return_type='list'):

        '''
        return unique data from list
        :param list: a list collection
        :param return_type: return object type (list,set etc.)
        :return: a list is produced by unique elements
        '''
        set_list = sorted(set(list_col), key=list_col.index)
        if return_type == 'list':
            return list(set_list)
        else:
            return set_list

    def divide_date_to_periods(self, df, date_column, period_arr):

        for period in period_arr:
            if period.lower() == "y":
                df["p_year"] = pd.DatetimeIndex(df[date_column]).year
            if period.lower() == "m":
                df["p_month"] = pd.DatetimeIndex(df[date_column]).month
            if period.lower() == "d":
                df["p_day"] = pd.DatetimeIndex(df[date_column]).day
            if period.lower() == "q":
                df['p_quarter'] = pd.DatetimeIndex(df[date_column]).quarter

        return df


