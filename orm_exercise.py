#
#   EXAM: HEALTHCARE INFORMATION SYSTEM 
#   NAME: Eleonora Zucchi
#


## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~III. ORM Exercise ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ 

from datetime import datetime, timedelta
from pony.orm import *
from load_data import *
import pandas as pd

# Creating database object and get connected
db = Database()
db.bind(provider='postgres', user='postgres', password='pwd', host='localhost', database='postgres')

file_path = '/Users/eleonorazucchi/Desktop/HIS/pmdata/participant-overview.xlsx'

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
# 1. Participant

class Participant(db.Entity):
    name = PrimaryKey(str)
    age = Optional(int)
    height = Optional(float)
    gender = Optional(str)
    
    baseline_bpm = Optional(int)            # min(value)
    baseline_calories = Optional(float)     # min(value)
    baseline_exercise = Optional(int)       # min(duration)
    baseline_sleep = Optional(int)          # min(duration)
    baseline_steps = Optional(int)          # min(value)
    baseline_distance = Optional(int)       # min(value)

    bpms = Set('BPM')
    calories = Set('Calories')
    exercise = Set('Exercise')
    sleep = Set('Sleep')
    steps = Set('Steps')
    distance = Set('Distance')
    day = Set('Day')

@db_session
def check_participant(name):
    with db_session:
        exists = Participant.exists(name=name)
    return exists


# Function thet given a filename loads all participants present in the file 
# and insert all of them inside the database
def add_all_participants(file_path): 
    
    dataframe  = pd.read_excel(file_path, header=1)        

    name = list(dataframe['Part'])  
    age = list(dataframe['Age'])
    height = list(dataframe['Height'])
    gender = list(dataframe['Gender'].Gender)
     
    for index in range(len(name)):
        try:  
            with db_session:
                p = Participant(
                    name = name[index], 
                    age = age[index], 
                    height = height[index], 
                    gender = gender[index],
                    baseline_calories = 1.39
                )   

        except TransactionIntegrityError:
            print("Caught IntegrityError: Primary key already exists in database")
            pass

    return print('Inserted row(s) in participants table')


# Function thet given a filename  and a participant name, loads all participants 
# present in the file and insert the chosen participant (name) inside the database
def add_new_participant(name, file_path): 
    
    dataframe  = pd.read_excel(file_path, header=1)        
        
    age = list(dataframe[ dataframe["Participant ID"] == name ].Age)[0]
    height = list(dataframe[ dataframe["Participant ID"] == name ].Height)[0]
    gender = list(dataframe[ dataframe["Participant ID"] == name ].Gender)[0]
     
    try:  
        with db_session:
            p = Participant(
                name = name, 
                age = age, 
                height = height, 
                gender = gender,
                #baseline_calories = 1.39
            )      

    except TransactionIntegrityError:
        print("Caught IntegrityError: Primary key already exists in database")
        pass

    return 

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
# 2. BPM

class BPM(db.Entity):
    vt = Required(datetime)                  # dateTime
    value = Optional(float, column='bpm')    # bpms
    participant = Required(Participant)      # name
    PrimaryKey(participant, vt)

def preprocess_BPM_day(name: str, date):
    dataframe  = load_json_data(participant=name, table_name='heart_rate') 
    dataframe["value"] = dataframe["value"].apply(lambda x: x['bpm']) 

    date = pd.to_datetime(date).date()        
    dataframe = dataframe[dataframe['dateTime'].dt.date == date] 
    dataframe = dataframe.mask(dataframe == '') 

    return dataframe

# Function that given a participant and a date loads data from file,
# filters them and inserts the chose rows (name, date) into bpm database tabale 
def add_BPM_day(name: str, dataframe):

    try:
        for index in range(len(dataframe['dateTime'])):  
            with db_session:  
                b = BPM(
                    vt = dataframe['dateTime'].iloc[index],
                    value = dataframe['value'].iloc[index],
                    participant = name
                ) 

    except TransactionIntegrityError:
        print("Caught IntegrityError: Primary key already exists in database")
        pass

    return 


## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
# 3. Calories

class Calories(db.Entity):
    vt = Required(datetime)                     # dateTime
    value = Optional(float, column='calories')  # calories
    participant = Required(Participant)         # name
    PrimaryKey(participant, vt)

def preprocess_calories_day(name: str, date):
    
    dataframe  = load_json_data(participant=name, table_name='calories')        
    date = pd.to_datetime(date).date()        
    dataframe = dataframe[dataframe['dateTime'].dt.date == date]  
    dataframe = dataframe.mask(dataframe == '') 

    return dataframe

# Function that given a participant and a date loads data from file,
# filters them and inserts the chose rows (name, date) into calories database tabale 
def add_calories_day(name: str, dataframe):

    try:
        for index in range(len(dataframe['dateTime'])):          
            with db_session: 
                c = Calories(
                    vt = dataframe['dateTime'].iloc[index],
                    value = dataframe['value'].iloc[index],
                    participant=name
                )  

    except TransactionIntegrityError:
        print("Caught IntegrityError: Primary key already exists in database")
        pass

    return 

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
# 4. Exercise

class Exercise(db.Entity):
    vts = Required(datetime)                                    #startTime
    vte = Optional(datetime)                                    #endTime
    activityName = Optional(float, column='activity_name')
    averageHeartRate = Optional(int, column='average_heart_ate')
    duration = Optional(int)
    participant = Required(Participant)
    PrimaryKey(participant, vts)

def preprocess_exercise_day(name: str, date):

    dataframe  = load_json_data(participant=name, table_name='exercise')        
    date = pd.to_datetime(date).date()        
    dataframe = dataframe[dataframe['startTime'] == date]  
    dataframe = dataframe.mask(dataframe == '') 

    dataframe['duration'] = pd.to_timedelta(dataframe['duration'], unit='ms')
    dataframe['endTime'] = dataframe['startTime'] + dataframe['activeDuration']

    return dataframe

def add_exercise_day(name: str, dataframe):

    try: 
        for index in range(len(dataframe['startTime'])):            
            with db_session:
                e = Exercise(
                    vts = dataframe['startTime'].iloc[index],
                    vte = dataframe['endTime'].iloc[index],
                    activityName = dataframe['activityName'].iloc[index],
                    averageHeartRate = dataframe['averageHeartRate'].iloc[index],
                    duration = dataframe['duration'].iloc[index],
                    participant=name
                )       
            
    except TransactionIntegrityError:
        print("Caught IntegrityError: Primary key already exists in database")
        pass

    return 

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
# 5. Sleep 

class Sleep(db.Entity):
    vts = Required(datetime)                    #startTime
    vte = Optional(datetime)                    #endTime
    value = Optional(int, column='efficiency')  #efficiency
    duration = Optional(int)
    participant = Required(Participant)
    PrimaryKey(participant, vts)

def preprocess_sleep_day(name: str, date):

    dataframe  = load_json_data(participant=name, table_name='sleep')        
    date = pd.to_datetime(date).date()        
    dataframe = dataframe[dataframe['dateOfSleep'] == date] 
    dataframe = dataframe.mask(dataframe == '')  

    return dataframe

def add_sleep_day(name: str, dataframe):

    try:
        for index in range(len(dataframe['startTime'])):    
            with db_session: 
                s = Sleep(
                    vts = dataframe['startTime'].iloc[index],
                    vte = dataframe['endTime'].iloc[index],
                    value = dataframe['efficiency'].iloc[index],
                    duration = dataframe['duration'].iloc[index],
                    participant=name
                )    
  
    except TransactionIntegrityError:
        print("Caught IntegrityError: Primary key already exists in database")
        pass
    except IndexError:
        print("Caught IndexError: Data not found")
        pass

    return 

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
# 6. Steps

class Steps(db.Entity):
    vt = Required(datetime)
    value = Optional(int, column='steps')
    participant = Required(Participant)
    PrimaryKey(participant, vt)

def preprocess_steps_day(name: str, date):

    dataframe  = load_json_data(participant=name, table_name='steps')        
    date = pd.to_datetime(date).date()        
    dataframe = dataframe[dataframe['dateTime'] == date] 
    dataframe = dataframe.mask(dataframe == '') 

    return dataframe

def add_steps_day(name: str, dataframe):

    try:
        for index in range(len(dataframe['dateTime'])):           
            with db_session: 
                s = Steps(
                    vt = dataframe['dateTime'].iloc[index],
                    value = dataframe['value'].iloc[index],
                    participant=name
                )  

    except TransactionIntegrityError:
        print("Caught IntegrityError: Primary key already exists in database")
        pass

    return 

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
# 7. Distance

class Distance(db.Entity):
    vt = Required(datetime)
    value = Optional(float, column='distance')
    participant = Required(Participant)
    PrimaryKey(participant, vt)

def preprocess_distance_day(name: str, date):

    dataframe  = load_json_data(participant=name, table_name='distance')        
    date = pd.to_datetime(date).date()        
    dataframe = dataframe[dataframe['dateTime'].dt.date == date]   
    dataframe = dataframe.mask(dataframe == '') 

    return dataframe

def add_distance_day(name: str, dataframe):

    try:
        for index in range(len(dataframe['dateTime'])): 
            with db_session:    
                s = Distance(
                    vt = dataframe['dateTime'].iloc[index],
                    value = dataframe['value'].iloc[index],
                    participant=name
                )  

    except TransactionIntegrityError:
        print("Caught IntegrityError: Primary key already exists in database")
        pass

    return 

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
# 8. Day

class Day(db.Entity):
    vt = Required(datetime)
    light_activity = Optional(int)
    moderate_activity = Optional(int)
    very_active_dataframe = Optional(int)
    sedentary = Optional(int)
    resting_heart_rate = Optional(float)
    sleep_score = Optional(int)
    participant = Required(Participant)   
    PrimaryKey(participant, vt)

def add_day(name: str, date): 

    date = pd.to_datetime(date).date()
    
    lightly_active_dataframe = load_json_data(participant=name, table_name='lightly_active_minutes')
    lightly_active_dataframe = lightly_active_dataframe[lightly_active_dataframe['dateTime'].dt.date == date]
    lightly_active_dataframe = lightly_active_dataframe.mask(lightly_active_dataframe == '') 

    moderately_active_dataframe = load_json_data(participant=name, table_name='moderately_active_minutes')
    moderately_active_dataframe = moderately_active_dataframe[moderately_active_dataframe['dateTime'].dt.date == date]
    moderately_active_dataframe = moderately_active_dataframe.mask(moderately_active_dataframe == '') 

    very_active_dataframe = load_json_data(participant=name, table_name='very_active_minutes')
    very_active_dataframe = very_active_dataframe[very_active_dataframe['dateTime'].dt.date == date]
    very_active_dataframe = very_active_dataframe.mask(very_active_dataframe == '') 

    sedentary_dataframe = load_json_data(participant=name, table_name='sedentary_minutes')
    sedentary_dataframe = sedentary_dataframe[sedentary_dataframe['dateTime'].dt.date == date]
    sedentary_dataframe = sedentary_dataframe.mask(sedentary_dataframe == '') 

    resting_heart_rate_dataframe = load_json_data(participant=name, table_name='resting_heart_rate')
    resting_heart_rate_dataframe = resting_heart_rate_dataframe[resting_heart_rate_dataframe['dateTime'].dt.date == date]
    resting_heart_rate_dataframe = resting_heart_rate_dataframe.mask(resting_heart_rate_dataframe == '') 

    sleep_score_dataframe = load_csv_data(participant=name, table_name='sleep_score')
    sleep_score_dataframe = sleep_score_dataframe[pd.to_datetime(sleep_score_dataframe['timestamp']).dt.date == date]


    if pd.isna(lightly_active_dataframe['value'].tolist()[0]):
        lightly_active_dataframe['value'].tolist()[0] = 0

    if pd.isna(moderately_active_dataframe['value'].tolist()[0]):
        moderately_active_dataframe['value'].tolist()[0] = 0

    if pd.isna(very_active_dataframe['value'].tolist()[0]):
        very_active_dataframe['value'].tolist()[0] = 0

    if pd.isna(sedentary_dataframe['value'].tolist()[0]):
        sedentary_dataframe['value'].tolist()[0] = 0

    if pd.isna(resting_heart_rate_dataframe['value'].tolist()[0]['value']):
        resting_heart_rate_dataframe['value'].tolist()[0]['value'] = 0

    if pd.isna(sleep_score_dataframe['overall_score'].tolist()[0]):
        sleep_score_dataframe['overall_score'].tolist()[0] = 0


    try:
        with db_session:

            d = Day(
                vt = pd.to_datetime(date), 
                light_activity = lightly_active_dataframe['value'].tolist()[0],
                moderate_activity = moderately_active_dataframe['value'].tolist()[0],
                very_active_dataframe = very_active_dataframe['value'].tolist()[0],
                sedentary = sedentary_dataframe['value'].tolist()[0],
                resting_heart_rate = resting_heart_rate_dataframe['value'].values[0]['value'],
                sleep_score = sleep_score_dataframe['overall_score'].tolist()[0],              
                participant = name
            )
            
    except TransactionIntegrityError:
        print("Caught IntegrityError: Primary key already exists in database")
        pass
    except IndexError:
        print("Caught IndexError: Data not found in table day")
        pass

    return 

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

def add_partecipant_day(name, day):

    with db_session:
        if(not check_participant(name=name)):
            add_new_participant(name=name, file_path=file_path)
            print("Added participant: " + name)

    with db_session:
        participant = Participant.get(name=name)
        print(participant)

        bpms = preprocess_BPM_day(name=name, date=day)
        min_bpms = bpms['value'].min()
        bpms['value'] = bpms['value'].apply(lambda x: x if x > min_bpms else None)
        add_BPM_day(name=name, dataframe=bpms)
        if pd.isna(min_bpms):
            min_bpms = 0

        participant.baseline_bpm = min_bpms

    with db_session:
        participant = Participant.get(name=name)
        calories = preprocess_calories_day(name=name, date=day)
        min_calories = calories['value'].min()
        calories['value'] = calories['value'].apply(lambda x: x if x > min_calories else None) 
        add_calories_day(name=name, dataframe=calories)  
        if pd.isna(min_calories):
            min_calories = 0
   
        participant.baseline_calories = min_calories
    
    with db_session:
        participant = Participant.get(name=name)
        exercise = preprocess_exercise_day(name=name, date=day)
        min_exercise = exercise['duration'].astype('timedelta64[ms]').min()
        exercise['duration'] = exercise['duration'].apply(lambda x: x if x > min_exercise else None) 
        add_exercise_day(name=name, dataframe=exercise)
        if pd.isna(min_exercise):
            min_exercise = 0

        participant.baseline_exercise = min_exercise

    with db_session:
        participant = Participant.get(name=name)
        sleep = preprocess_sleep_day(name=name, date=day)
        min_sleep = sleep['efficiency'].min()
        sleep['efficiency'] = sleep['efficiency'].apply(lambda x: x if x > min_sleep else None)
        add_sleep_day(name=name, dataframe=sleep)
        if pd.isna(min_sleep):
            min_sleep = 0
        participant.baseline_sleep = min_sleep

    with db_session:
        participant = Participant.get(name=name)
        steps = preprocess_steps_day(name=name, date=day)
        min_steps = steps['value'].min()
        steps['value'] = steps['value'].apply(lambda x: x if x > min_steps else None)
        add_steps_day(name=name, dataframe=steps)
        if pd.isna(min_steps):
            min_steps = 0
        participant.baseline_steps = min_steps

    with db_session:
        participant = Participant.get(name=name)
        distance = preprocess_distance_day(name=name, date=day)
        min_distance = distance['value'].min()
        distance['value'] = distance['value'].apply(lambda x: x if x > min_distance else None)
        add_distance_day(name=name, dataframe=distance)
        if pd.isna(min_distance):
            min_distance = 0
        distance.baseline_distance = min_distance

        add_day(name=name, date=day)
    return

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

@db_session
def time_aggregation(name, vts, vte, minutes):

    vts = pd.to_datetime(vts)
    vte = pd.to_datetime(vte)
    
    ## * BPM
    bpms = load_json_data(participant=name, table_name='heart_rate')
    bpms["value"] = bpms["value"].apply(lambda x: x['bpm']) 
    bpms = bpms.set_index('dateTime')
    bpms = select_interval(dataframe=bpms, vts=vts, vte=vte)

    aggregated_bpms = aggregate_data(
        dataframe=bpms, 
        minutes=minutes, 
        kwargs={'value': 'mean',}
    )

    ## * Calories
    calories = load_json_data(participant=name, table_name='calories')
    calories = calories.set_index('dateTime')
    calories = select_interval(dataframe=calories, vts=vts, vte=vte)
  
    aggregated_calories = aggregate_data(
        dataframe=calories, 
        minutes=minutes,
        kwargs={'value': 'sum'}
    )

    ## * Exercise
    exercise = load_json_data(participant=name, table_name='exercise')
    exercise['startTime'] = pd.to_datetime(exercise['startTime'])
    exercise = exercise.set_index('startTime')
    exercise = select_interval(dataframe=exercise, vts=vts, vte=vte)

    exercise['duration'] = pd.to_timedelta(exercise['duration'], unit='ms')
    exercise['endTime'] = exercise.index + exercise['duration']

    aggregated_exercise = aggregate_data(
        dataframe=exercise, 
        minutes=minutes,
        kwargs={'activityName':'last', 'duration':'last', 'endTime':'last', 'averageHeartRate':'mean'}
    )

    ## * Sleep
    sleep = load_json_data(participant=name, table_name='sleep')
    sleep['startTime'] = pd.to_datetime(sleep['startTime'])
    sleep = sleep.set_index('startTime')
    sleep = select_interval(dataframe=sleep, vts=vts, vte=vte)
    
    exercise['duration'] = pd.to_timedelta(exercise['duration'], unit='ms')
    exercise['endTime'] = exercise.index + exercise['duration']

    ## * Steps
    steps = load_json_data(participant=name, table_name='steps')
    steps = steps.set_index('dateTime')
    steps = select_interval(dataframe=steps, vts=vts, vte=vte)

    aggregated_steps = aggregate_data(
        dataframe=steps, 
        minutes=minutes,
        kwargs={'value': 'sum'})

    ## * Distance
    distance = load_json_data(participant=name, table_name='steps')
    distance = distance.set_index('dateTime')
    distance = select_interval(dataframe=distance, vts=vts, vte=vte)

    aggregated_distance = aggregate_data(
        dataframe=distance, 
        minutes=minutes,
        kwargs={'value': 'sum'})

    result = pd.merge_asof(aggregated_bpms, aggregated_calories, on='dateTime')
    result = pd.merge_asof(result, aggregated_steps, on='dateTime')
    result = pd.merge_asof(result, aggregated_distance, on='dateTime')
    result = pd.merge(result, aggregated_exercise, left_on='dateTime', right_on='startTime', how="outer")
    result = copy_activity(result)
    result['participant'] = name

    return result

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

db.generate_mapping(create_tables=True)

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~


try:
    add_new_participant(name='p02', file_path=file_path)
    #add_partecipant_day(name='p01', day='2019-11-01')
    #add_partecipant_day(name='p01', day='2019-11-02')
    #add_partecipant_day(name='p01', day='2019-11-03')
    #add_partecipant_day(name='p06', day='2019-12-20')
    #add_partecipant_day(name='p06', day='2019-12-21')
    #add_partecipant_day(name='p03', day='2019-11-09')
    add_partecipant_day(name='p04', day='2019-11-03')
except IndexError:
    pass
except TransactionIntegrityError:
    pass

db.commit()
## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

aggregated_dataframe = time_aggregation(name='p01', vts='2019-11-25', vte='2019-11-26', minutes='10')
print(aggregated_dataframe)

## ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

db.disconnect()
