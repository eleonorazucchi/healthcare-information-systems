#
#   EXAM: HEALTHCARE INFORMATION SYSTEM 
#   NAME: Eleonora Zucch
#

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

import os.path
import pandas as pd

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

# Function that loads .json files
def load_json_data(participant, table_name: str):

    path = os.path.join('pmdata', participant, 'fitbit', table_name + '.json')
    dataframe = pd.read_json(path)

    return dataframe

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

# Function that loads .csv files
def load_csv_data(participant, table_name):

    path = os.path.join('pmdata', participant, 'fitbit', table_name + '.csv')
    dataframe = pd.read_csv(path)

    return dataframe

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

def aggregate_data(dataframe, minutes: int, kwargs):
    freq = str(minutes) + 'T'

    dataframe.index = dataframe.index.round(freq=freq)
    dataframe = dataframe.groupby(by=dataframe.index, as_index=1).agg(kwargs)
    dataframe = dataframe.resample(freq).ffill()
       
    return dataframe 

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

def select_interval(dataframe, vts, vte):

    dataframe = dataframe.iloc[dataframe.index >= vts]
    dataframe = dataframe.iloc[dataframe.index < vte] 

    return dataframe

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

def copy_activity(dataframe):
    for index, row in dataframe.iterrows():
        if(index > 0):
            if (dataframe.loc[index-1, 'endTime'] >= dataframe.loc[index, 'dateTime']):
                dataframe.loc[index, 'endTime']  = dataframe.loc[index-1, 'endTime'] 
                dataframe.loc[index, 'duration']  = dataframe.loc[index-1, 'duration'] 
                dataframe.loc[index, 'activityName'] = dataframe.loc[index-1, 'activityName']
                dataframe.loc[index, 'averageHeartRate'] = dataframe.loc[index-1, 'averageHeartRate']
    return dataframe


