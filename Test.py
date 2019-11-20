from multiprocessing import freeze_support

from dunner.data_cleaner import DataCleaner
from dunner.data_operations import DataOperations
import pandas as pd
import dunner.preprocess_helper
import pandas as pd
import time
import numpy as np
import datetime
from datetime import timedelta


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

def change_str_column(str_series):

    if str_series.dtype == 'object':
        #str_series = str_series.replace({'Riti':'Emre'})
        str_series = str_series.apply(lambda row: '22SE' if row == '22E' else row)

    return str_series


if __name__ == '__main__':

    # conditional data cleaner test
    dc = DataCleaner()
    record = {

        'Name': ['Ankit', 'Amit', 'Aishwarya', 'Priyanka', 'Priya', 'Shaurya'],
        'Age': [21, 19, 20, 18, 17, 21],
        'Stream': ['Math', 'Commerce', 'Science', 'Math', 'Math', 'Science'],
        'Percentage': [88, 92, 95, 70, 65, 78]}

    # create a dataframe
    dataframe = pd.DataFrame(record, columns=['Name', 'Age', 'Stream', 'Percentage'])

    # dict = {'Age': 'x>17', 'Stream': 'x==\'Math\'', 'Percentage': 'x>70'}
    #
    # new_df = dc.remove_rows_by_condition(dataframe, dict)

    #region parallel process test
    #df = pd.read_csv('datathon_case_1_case_1_market_data.csv')
    #test = helper.Helper.parallelize(df, parallel_test_func)
    #endregion

    #region pipeline test
    #func_list = {"sum":sum,"multiply":multiply}
    #helper = helper.Helper()
    #helper.pipeline(func_list)
    #endregion

    #region outlier remove test
    # dt = DataCleaner()
    # np.random.seed(42)
    # age = np.random.randint(20, 100, 50)
    # name = ['name' + str(i) for i in range(50)]
    # address = ['address' + str(i) for i in range(50)]
    # df = pd.read_csv('datathon_case_1_case_1_market_data.csv') #pd.DataFrame(data={'age': age, 'name': name, 'address': address})
    # removed_df = dt.remove_outliers(df, .05,.95)
    #endregion

    #region Faster data read
    # start_time = time.time()
    # df = pd.read_csv('datathon_case_1_case_1_market_data.csv')
    # elapsed_time = time.time() - start_time
    # print(str.format("Pandas geçen zaman {0}", time.strftime("%H:%M:%S", time.gmtime(elapsed_time))))
    # do = DataOperations()
    # start_time = time.time()
    # df_f = do.read_csv_file('datathon_case_1_case_1_market_data.csv')
    # elapsed_time = time.time() - start_time
    # print(str.format("Pandas geçen zaman {0}", time.strftime("%H:%M:%S", time.gmtime(elapsed_time))))
    #endregion

    #region unique data test
    # lst = ['word1','word2','word3']
    #     # do=DataOperations()
    #     # rtr = do.return_unique_data(lst)
    #endregion

    #region Divide Date to Season Test
    # date_today = datetime.datetime.now()
    # days = pd.date_range(date_today, date_today + timedelta(7), freq='D')
    #
    # np.random.seed(seed=1111)
    # data = np.random.randint(1, high=100, size=len(days))
    # df = pd.DataFrame({'test': days, 'col2': data})
    # do = DataOperations()
    # do.divide_date_to_periods(df,'test',['y','M','d','Q'])
    #endregion

    #region Convert to datetime test section
    # dates = ['2015-08-10','2015-08-11','2015-08-12']
    # df = pd.DataFrame({'test': dates})
    # do = DataOperations()
    # df = do.to_datetime(df,'test')
    #
    # print(df)
    #endregion

    # students = [('jack', 34, 'Sydeny', 34, 'Sydeny', 34),
    #             ('Riti', 30, 'Delhi', 30, 'Delhi', 30),
    #             ('Aadi', 16, 'New York', 16, 'New York', 16),
    #             ('Riti', 30, 'Delhi', 30, 'Delhi', 30),
    #             ('Riti', 30, 'Delhi', 30, 'Delhi', 30),
    #             ('Riti', 30, 'Mumbai', 30, 'Mumbai', 30),
    #             ('Aadi', 40, 'London', 40, 'London', 40),
    #             ('Sachin', 30, 'Delhi', 30, 'Delhi', 30)
    #             ]
    #
    # df = pd.DataFrame(students, columns=['Name', 'Age', 'City', 'Marks', 'Address', 'Pin'])

    do = DataOperations()
    nex_df = do.change_obj_to_num(dataframe, limit=6)

    dc = DataCleaner()
    df = dc.remove_duplicates(df)

    print("End")

#helper.Helper.parallelize(data=df, func = parallel_test_func)

#print("End")
