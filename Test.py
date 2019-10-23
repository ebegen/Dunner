from data_cleaner import DataCleaner
import pandas as pd

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

print("End")
