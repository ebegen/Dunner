from multiprocessing import freeze_support

from data_cleaner import DataCleaner
from data_operations import DataOperations
import pandas as pd
import helper
import pandas as pd
import time
import numpy as np



df = pd.read_csv('datathon_case_1_case_1_market_data.csv')

count=0
def parallel_test_func(df):
    df['test'] = df['SEASON']*2
    global count
    #df['count'] = count
    count+=1
    print(count)
    return df

def sum():
    a = 2
    b = 3
    print(a + b)

def multiply():
    a = 2
    b = 3
    print(a * b)

if __name__ == '__main__':

    # conditional data cleaner test
    # dc = DataCleaner()
    # record = {
    #
    #     'Name': ['Ankit', 'Amit', 'Aishwarya', 'Priyanka', 'Priya', 'Shaurya'],
    #     'Age': [21, 19, 20, 18, 17, 21],
    #     'Stream': ['Math', 'Commerce', 'Science', 'Math', 'Math', 'Science'],
    #     'Percentage': [88, 92, 95, 70, 65, 78]}
    #
    # # create a dataframe
    # dataframe = pd.DataFrame(record, columns=['Name', 'Age', 'Stream', 'Percentage'])
    #
    # dict = {'Age': 'x>17', 'Stream': 'x==\'Math\'', 'Percentage': 'x>70'}
    #
    # new_df = dc.remove_rows_by_condition(dataframe, dict)

    #parallel process test
    #df = pd.read_csv('datathon_case_1_case_1_market_data.csv')
    #test = helper.Helper.parallelize(df, parallel_test_func)

    #pipeline test
    #func_list = {"sum":sum,"multiply":multiply}
    #helper = helper.Helper()
    #helper.pipeline(func_list)

    #outlier remove test
    # dt = DataCleaner()
    # np.random.seed(42)
    # age = np.random.randint(20, 100, 50)
    # name = ['name' + str(i) for i in range(50)]
    # address = ['address' + str(i) for i in range(50)]
    # df = pd.read_csv('datathon_case_1_case_1_market_data.csv') #pd.DataFrame(data={'age': age, 'name': name, 'address': address})
    # removed_df = dt.remove_outliers(df, .05,.95)

    #Faster data read
    # start_time = time.time()
    # df = pd.read_csv('datathon_case_1_case_1_market_data.csv')
    # elapsed_time = time.time() - start_time
    # print(str.format("Pandas geçen zaman {0}", time.strftime("%H:%M:%S", time.gmtime(elapsed_time))))
    # do = DataOperations()
    # start_time = time.time()
    # df_f = do.read_csv_file('datathon_case_1_case_1_market_data.csv')
    # elapsed_time = time.time() - start_time
    # print(str.format("Pandas geçen zaman {0}", time.strftime("%H:%M:%S", time.gmtime(elapsed_time))))

    #Return unique elements from list
    lst = ['word1','word2','word3']
    do=DataOperations()
    rtr = do.return_unique_data(lst)

    print("End")

#helper.Helper.parallelize(data=df, func = parallel_test_func)

#print("End")
