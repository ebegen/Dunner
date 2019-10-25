from multiprocessing import freeze_support

from data_cleaner import DataCleaner
import pandas as pd
import helper
import pandas as pd
import timeit


dc = DataCleaner()

record = {

    'Name': ['Ankit', 'Amit', 'Aishwarya', 'Priyanka', 'Priya', 'Shaurya'],
    'Age': [21, 19, 20, 18, 17, 21],
    'Stream': ['Math', 'Commerce', 'Science', 'Math', 'Math', 'Science'],
    'Percentage': [88, 92, 95, 70, 65, 78]}

# create a dataframe
dataframe = pd.DataFrame(record, columns=['Name', 'Age', 'Stream', 'Percentage'])

dict = {'Age':'x>17', 'Stream':'x==\'Math\'', 'Percentage':'x>70'}

new_df = dc.remove_rows_by_condition(dataframe,dict)

df = pd.read_csv('datathon_case_1_case_1_market_data.csv')

count=0
def parallel_test_func(df):
    df['test'] = df['SEASON']*2
    global count
    #df['count'] = count
    count+=1
    print(count)
    return df


if __name__ == '__main__':
    #df = pd.read_csv('datathon_case_1_case_1_market_data.csv')
    test = helper.Helper.parallelize(df, parallel_test_func)
    print("End")

#helper.Helper.parallelize(data=df, func = parallel_test_func)

#print("End")
