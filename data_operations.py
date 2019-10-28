import feather
import dask.dataframe as dd

class DataOperations:

    def read_csv_file(self,file_path):
        data = dd.read_csv(file_path)
        df = data.compute()
        return df