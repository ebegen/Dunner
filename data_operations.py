import feather
import dask.dataframe as dd

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

