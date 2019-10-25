import numpy as np
import pandas as pd
from multiprocessing import cpu_count, Pool
import dask.dataframe as ddf
import swifter

class Helper:

    def parallelize(data, func):
        # cores = cpu_count()  # Number of CPU cores on your system
        # partitions = cores-2  # Define as many partitions as you want
        #
        # data_split = np.array_split(data, partitions)
        # pool = Pool(cores)
        # data = pd.concat(pool.map(func, data_split))
        # pool.close()
        # pool.join()

        #df_dask = ddf.from_pandas(data, npartitions=cpu_count()-2)
        #new_data = df_dask.apply(lambda x: func(x), meta=('str'), axis=1).compute(scheduler='multiprocessing')

        new_data = data.swifter.apply(lambda row: func(row), axis=1)

        return new_data
