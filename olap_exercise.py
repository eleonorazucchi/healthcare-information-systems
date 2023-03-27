#
# EXAM: HEALTHCARE INFORMATION SYSTEM 
# NAME: Eleonroa Zucchi
# 
# II. OLAP Exercise

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

import pandas as pd
import openpyxl
import os.path
import psycopg2
from datetime import datetime
import numpy as np
from table_model import *


index_path = '/Users/eleonorazucchi/Desktop/HIS/EXAM/pmdata/participant-overview.xlsx'

# Read the excel file and insert all participants
participants = pd.read_excel(index_path, header=1)
# insert_participant_olap(dataframe=participants)


# Function that loads .json files
def load_json_data(participant, table_name: str):
 
    dataframe =[]
    path = os.path.join('pmdata', participant, 'fitbit', table_name + '.json')
    try:
        dataframe = pd.read_json(path)
        #dataframe = dataframe.set_index(index)     
    except ValueError:
        raise ValueError 

    return dataframe

# Function that loads .csv files
def load_csv_data(participant, table_name):
    path = os.path.join('pmdata', participant, 'fitbit', table_name + '.csv')
    dataframe = pd.read_csv(path)
    return dataframe

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

# A. Exercise

def preprocess_exercise_data(dataframe, participant, age, height, gender):
    dataframe['participant'] = participant
    dataframe['age'] = age
    dataframe['height'] = height
    dataframe['gender'] = gender
    dataframe['duration'] = pd.to_timedelta(dataframe['duration'], unit='ms')
    dataframe['startTime'] = pd.to_datetime(dataframe['startTime'])
    dataframe['weekday'] = dataframe['startTime'].dt.day_name()
    dataframe['weekend'] = dataframe['startTime'].dt.dayofweek > 4

    dataframe = dataframe[['startTime', 'participant', 'age', 'height', 'gender', 'activityName', 'calories', 'duration', 'steps', 'elevationGain', 'weekday', 'weekend']]
    return dataframe


for index, row in participants.iterrows():
    name = participants.loc[index, 'Participant ID']
    age = participants.loc[index, 'Age']
    height = participants.loc[index, 'Height']
    gender = participants.loc[index, 'Gender']
    exercise = load_json_data(participant=name, table_name='exercise')

    exercise = preprocess_exercise_data(dataframe=exercise, participant=name, age=age, height=height, gender=gender)
    insert_exercise_olap(dataframe=exercise)

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

# B. Sleep

def preprocess_sleep_data(dataframe, participant, age, height, gender):
    dataframe['participant'] = participant
    dataframe['age'] = age
    dataframe['height'] = height
    dataframe['gender'] = gender
    dataframe['timestamp']= pd.to_datetime(dataframe['timestamp'])
    dataframe['wake_up_hour'] = dataframe['timestamp'].dt.time
    dataframe['day'] = dataframe['timestamp']
    dataframe['weekday'] = dataframe['timestamp'].dt.day_name()
    dataframe['weekend'] = dataframe['timestamp'].dt.dayofweek > 4
    
    dataframe = dataframe[['participant', 'age', 'height', 'gender', 'timestamp', 'wake_up_hour', 'overall_score', 'duration_score', 'deep_sleep_in_minutes', 'resting_heart_rate', 'day', 'weekday', 'weekend']]
    return dataframe

for index, row in participants.iterrows():
    name = participants.loc[index, 'Participant ID']
    age = participants.loc[index, 'Age']
    height = participants.loc[index, 'Height']
    gender = participants.loc[index, 'Gender']
    sleep = load_csv_data(participant=name, table_name='sleep_score')

    sleep = preprocess_sleep_data(dataframe= sleep, participant=name, age=age, height=height, gender=gender)
    insert_sleep_olap(dataframe=sleep)

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

# C. ActiveMinutes

def preprocess_active_data(dataframe, participant, age, height, gender):
    
    dataframe['participant'] = participant
    dataframe['age'] = age
    dataframe['height'] = height
    dataframe['gender'] = gender
    dataframe['dateTime']= pd.to_datetime(dataframe['dateTime'])
    dataframe['day'] = dataframe['dateTime']
    dataframe['weekday'] = dataframe['dateTime'].dt.day_name()
    dataframe['weekend'] = dataframe['dateTime'].dt.dayofweek > 4
    
    dataframe = dataframe[['participant', 'age', 'height', 'gender', 'sedentary', 'lightly', 'moderately', 'very_active', 'dateTime', 'day', 'weekday', 'weekend']]
    return dataframe

for index, row in participants.iterrows():
    name = participants.loc[index, 'Participant ID']
    age = participants.loc[index, 'Age']
    height = participants.loc[index, 'Height']
    gender = participants.loc[index, 'Gender']
    sedentary_minutes = load_json_data(participant=name, table_name='sedentary_minutes')
    sedentary_minutes.rename(columns={'value':'sedentary'}, inplace=True)

    try:
        lightly_active_minutes = load_json_data(participant=name, table_name='lightly_active_minutes')
        lightly_active_minutes.rename(columns={'value':'lightly'}, inplace=True)
    except ValueError:
        pass

    moderately_active_minutes = load_json_data(participant=name, table_name='moderately_active_minutes')
    moderately_active_minutes.rename(columns={'value':'moderately'}, inplace=True)

    very_active_minutes = load_json_data(participant=name, table_name='very_active_minutes')
    very_active_minutes.rename(columns={'value':'very_active'}, inplace=True)

    active_minutes = pd.merge_asof(sedentary_minutes, lightly_active_minutes, on='dateTime')
    active_minutes = pd.merge_asof(active_minutes, moderately_active_minutes, on='dateTime')
    active_minutes = pd.merge_asof(active_minutes, very_active_minutes, on='dateTime')

    active_minutes = preprocess_active_data(dataframe = active_minutes, participant=name, age=age, height=height, gender=gender)
    insert_active_minutes_olap(dataframe=active_minutes)


## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

query_A = "SELECT date, SUM(sedentary), SUM(lightly), SUM(moderate), SUM(very_active) \
	       FROM public.activeminutes_olap \
	       WHERE DATE_PART('month', DATE_TRUNC('month', date)) = 11 AND participant_id = 'p01' \
	       GROUP BY date;"

with db_session:
    result_A = db.select(query_A)
    dataframe_A = pd.DataFrame(result_A, columns=['date', 'sedentary', 'lightly', 'moderate', 'very_active'])
    print(dataframe_A)

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

query_B = "SELECT participant_id, AVG(wake_up_hour) AS weekly_average_hour, \
           AVG(overall_score) AS weekly_average_score, \
           DATE_TRUNC('week', date), weekend \
           FROM public.sleep_olap \
           WHERE age > 50  \
           GROUP BY participant_id, DATE_TRUNC('week', date), weekend \
           ORDER BY DATE_TRUNC('week', date);"

with db_session:
    result_B = db.select(query_B)
    dataframe_B = pd.DataFrame(result_B, columns=['weekly_average_hour', 'weekly_average_score', 'date', 'weekend'])
    print(dataframe_B)

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

query_C = "SELECT AVG(duration) AS avg_duration, COUNT(activity_type) AS no_exercises, day_of_the_week \
	       FROM public.exercise_olap \
	       WHERE participant_id = 'p01' \
	       GROUP BY day_of_the_week;"

with db_session:
    result_C = db.select(query_C)
    dataframe_C = pd.DataFrame(result_C, columns=['timedelta', 'value', 'weekday'])
    print(dataframe_C)

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
