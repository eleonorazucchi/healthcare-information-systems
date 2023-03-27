#
# EXAM: HEALTHCARE INFORMATION SYSTEM 
# NAME: Eleonroa Zucchi
# 
# II. OLAP Exercise

from datetime import datetime, timedelta
from datetime import time
import pandas as pd
from pony.orm import *

# Creating database object and get connected
db = Database()
db.bind(provider='postgres', user='postgres', password='pwd', host='localhost', database='postgres')

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

class Exercise_olap(db.Entity):
    participant_id = Required(str)
    age = Optional(int)
    height = Optional(float)
    gender = Optional(str)
    calories = Optional(int)
    duration = Optional(timedelta)
    num_steps = Optional(int)
    elevation_gain = Optional(float)
    activity_type = Optional(str)
    start_time = Required(datetime)
    day_of_the_week = Optional(str)
    weekend = Optional(bool)
    PrimaryKey(participant_id, start_time)

class Sleep_olap(db.Entity):
    participant_id = Required(str)
    age = Optional(int)
    height = Optional(float)
    gender = Optional(str)
    date = Required(datetime)
    wake_up_hour = Optional(time)
    overall_score = Optional(int)
    duration_score = Optional(int)
    deep_sleep_in_minutes = Optional(int)
    rest_heart_rate = Optional(int)
    day_of_the_week = Optional(str)
    weekend = Optional(bool)
    PrimaryKey(participant_id, date)


class ActiveMinutes_olap(db.Entity):
    participant_id = Required(str)
    age = Optional(int)
    height = Optional(float)
    gender = Optional(str)
    date = Required(datetime)
    sedentary = Optional(int)
    lightly = Optional(int)
    moderate = Optional(int)
    very_active = Optional(int)
    day_of_the_week = Optional(str)
    weekend = Optional(bool)
    PrimaryKey(participant_id, date)

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

def insert_exercise_olap(dataframe):
    dataframe['steps'] = dataframe['steps'].apply(lambda x: 0 if pd.isna(x) else int(x))

    for index, row in dataframe.iterrows():
        try:
            with db_session:
                p = Exercise_olap(
                    participant_id = dataframe.loc[index, 'participant'],
                    age = dataframe.loc[index, 'age'],
                    height = dataframe.loc[index, 'height'],
                    gender = dataframe.loc[index, 'gender'],
                    calories = dataframe.loc[index, 'calories'],
                    duration = dataframe.loc[index, 'duration'],
                    num_steps = dataframe.loc[index, 'steps'],
                    elevation_gain = dataframe.loc[index, 'elevationGain'],
                    activity_type = dataframe.loc[index, 'activityName'],
                    start_time = dataframe.loc[index, 'startTime'],
                    day_of_the_week = dataframe.loc[index, 'weekday'],
                    weekend = dataframe.loc[index, 'weekend']
                )
        except TransactionIntegrityError:
            pass
    return   

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

def insert_sleep_olap(dataframe):
    for index, row in dataframe.iterrows():
        try:
            with db_session:
                s = Sleep_olap(
                    participant_id = dataframe.loc[index, 'participant'],
                    age = dataframe.loc[index, 'age'],
                    height = dataframe.loc[index, 'height'],
                    gender = dataframe.loc[index, 'gender'],
                    date = dataframe.loc[index, 'day'],
                    wake_up_hour = dataframe.loc[index, 'wake_up_hour'],
                    overall_score = dataframe.loc[index, 'overall_score'],
                    duration_score = dataframe.loc[index, 'duration_score'],
                    deep_sleep_in_minutes = dataframe.loc[index, 'deep_sleep_in_minutes'],
                    rest_heart_rate = dataframe.loc[index, 'resting_heart_rate'],
                    day_of_the_week = dataframe.loc[index, 'weekday'],
                    weekend = dataframe.loc[index, 'weekend']
                )
        except TransactionIntegrityError:
            pass
    return   

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

def insert_active_minutes_olap(dataframe):
    for index, row in dataframe.iterrows():
        try:
            with db_session:
                a = ActiveMinutes_olap(
                    participant_id = dataframe.loc[index, 'participant'],
                    age = dataframe.loc[index, 'age'],
                    height = dataframe.loc[index, 'height'],
                    gender = dataframe.loc[index, 'gender'],
                    date = dataframe.loc[index, 'day'],
                    sedentary = dataframe.loc[index, 'sedentary'],
                    lightly = dataframe.loc[index, 'lightly'],
                    moderate = dataframe.loc[index, 'moderately'],
                    very_active = dataframe.loc[index, 'very_active'],
                    day_of_the_week = dataframe.loc[index, 'weekday'],
                    weekend = dataframe.loc[index, 'weekend'],
                )
        except TransactionIntegrityError:
            pass
    return 
db.generate_mapping(create_tables=True)
