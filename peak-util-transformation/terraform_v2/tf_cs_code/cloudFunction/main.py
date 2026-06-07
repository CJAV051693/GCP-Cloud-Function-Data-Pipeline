import json
import base64
import requests
import logging
import string
import base64
import fileinput
import logging
import functions_framework
import numpy as np
import pandas as pd
import datetime as dt
import time as std_time
import gc

from datetime import time
from io import BytesIO
from google.cloud import storage
from google.cloud import secretmanager
from google.cloud import bigquery
from zoneinfo import ZoneInfo
from google.cloud import scheduler_v1

def gcp2df_(sql, client):
    query = client.query(sql)
    results = query.result()
    return results.to_dataframe()


def CreateListOfLists(client, MonthList, batchno, datalake_id, dataset_id, table_id3):    

    #GetBatchlist
    try:
        sql = f"""
        SELECT *
        FROM `{datalake_id}.{dataset_id}.{table_id3}`
        """
        df = gcp2df_(sql, client)
        df['month'] = df['month'].astype(str)
        print(df)
        print("Successfully get batchlist")
    except Exception as e:
        print(f"Error getting batchlist: {str(e)}")

    #Set Variables
    try:
        bl = []
        m = batchno
        n = m-1
        bm = f"batch{m}"
        bn = f"batch{n}"
        print("Successfully set variables")
    except Exception as e:
        print(f"Error setting variables: {str(e)}")

    # Start looping for list of lists    
    try:
        for month in MonthList:

            if m == 1:
                a = df.loc[df['month'] == month , bm].iloc[0]
                batch = list(range(1, a+1))

            else:
                a = df.loc[df['month'] == month , bn].iloc[0]
                b = df.loc[df['month'] == month , bm].iloc[0]
                batch = list(range(a+1, b+1))

            bl.append(batch)
        print("Successfully started looping for list of lists")
    except Exception as e:
        print(f"Error starting looping for list of lists: {str(e)}")

    print(bl)

    return bl


def GetTable(client, datalake_id, dataset_id, table_id):

    
    sql = f"""
    SELECT *
    FROM `{datalake_id}.{dataset_id}.{table_id}`
    """
    df_status = gcp2df_(sql, client)

        
    return df_status


def GetCcureData(client, datalake_id, dataset_id, table_id1, month, cb, lb):

    print(f"filtering groups between {cb} and {lb}")

    sql = f"""
    SELECT *
    FROM `{datalake_id}.{dataset_id}.{table_id1}`
    WHERE
        month = DATE('{month}')
        AND `Group` >= {cb} 
        AND `Group` <= {lb}
    ORDER BY `Group` ASC
    """
    query_job = client.query(sql)

    
    return query_job

def write_to_bigquery(client, datalake_id, dataset_id, table_id_target, json_data):
    
    # Convert transformed data to BigQuery rows
    table_ref = f"{datalake_id}.{dataset_id}.{table_id_target}"


    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Overwrites the table
    )

    # Upload data
    job = client.load_table_from_json(
        json_data, table_ref, job_config=job_config
    )
    job.result()



def write_to_bigquery_append(dtr, step, datalake_id, dataset_id, table_id_target, client, month, counter, grp_no):

    destination_table = f"{datalake_id}.{dataset_id}.{table_id_target}"

    # Convert all columns to string format
    try:
        dtr = dtr.astype(str)
        print("Successfully convert all columns to string")
    except Exception as e:
        print(f"Error converting all columns to string: {str(e)}")


    # Convert dataframe to json format
    try:
        json_data = dtr.to_dict(orient="records")
        print("Successfully convert dataframe to json")
    except Exception as e:
        print(f"Error converting dataframe to json: {str(e)}")

    # Write fully transformed data
    try:
        client.load_table_from_json(json_data, destination_table).result()
        del dtr, json_data; gc.collect()
        print("Successfully write fully transformed table")
        print(month)
        print(counter)
        print(f"Group {counter} {step} written.")
        print(f"Group* {grp_no} {step} written.")
    except Exception as e:
        print(f"Error writing Group {counter} {step} table: {str(e)}")

    






def dft_raw(table):

    df_raw = table

    try:
        tz = df_raw['localtimestamp'].dt.tz
        print(f"at df_raw step the timezone status is {tz}")
    except Exception as e:
        print(f"Error getting timezone status at df_raw step: {str(e)}")


    df_raw.dropna(how="all", inplace=True)
    df_raw.localtimestamp = pd.to_datetime(df_raw.localtimestamp, format="%Y-%m-%d %H:%M:%S", errors='coerce')
    df_raw['LocalTime'] = df_raw['localtimestamp'].dt.strftime("%H:%M:%S")
    df_raw['DateOnly'] = df_raw['localtimestamp'].dt.strftime("%Y-%m-%d")
    df_raw.drop_duplicates(inplace=True)
    df_raw.loc[:, 'door'] = df_raw.loc[:, 'door'].str.replace(" ", "")
    df_raw['Facility'] = (df_raw.door.str[0:11:1]).str.upper()
    # Checking Missing EID
    df_raw.loc[:, 'eid'] = df_raw.loc[:, 'eid'].str.lower()
    df_raw.loc[:, 'eid'] = df_raw.loc[:, 'eid'].str.replace(" ", "")
    # Dropping Missing EID
    df_raw.dropna(subset=['eid'], inplace=True)
    # Dropping Missing Door
    df_raw.dropna(subset=['door'], inplace=True)
    df_raw.loc[:, 'direction'] = df_raw.loc[:, 'direction'].str.lower()
    df_raw.loc[:, 'direction'] = df_raw.loc[:, 'direction'].str.replace(" ", "")
    # Dropping Missing Direction
    df_raw.dropna(subset=['direction'], inplace=True)
    df_raw.drop(columns=['messagetype'], inplace=True)
    # Drop columns that are not needed
    df_raw.drop(columns=['door'], inplace=True)
    

    # Derive Date
    df_raw.loc[:, 'DateOnly'] = df_raw.loc[:, 'localtimestamp'].dt.date
    df_raw.sample(2)

    # OFFSET BADGES AT EXACTLY 12 MIDNIGHT
    # Offset timestamp = 0:00:00 by 1 minute depending on Direction of Badge

    df_raw.loc[:, 'localtimestamp'] = np.where(
        (df_raw.loc[:, 'localtimestamp'].dt.hour == 0) & (df_raw.loc[:, 'localtimestamp'].dt.minute == 0) & (
                df_raw.loc[:, 'direction'] == "in"), \
        df_raw["localtimestamp"] + dt.timedelta(minutes=1),
        np.where(
            (df_raw.loc[:, 'localtimestamp'].dt.hour == 0) & (df_raw.loc[:, 'localtimestamp'].dt.minute == 0) & (
                    df_raw.loc[:, 'direction'] == "out"), \
            df_raw.localtimestamp - dt.timedelta(minutes=1), df_raw.localtimestamp)
    )

    return df_raw

def dft_cleaned(table):

    df_cleaned = table
    # Create a tag indicator for indirection
    df_cleaned.loc[:, 'IN'] = 0
    df_cleaned.loc[:, 'IN'] = np.where(df_cleaned.loc[:, 'direction'] == 'in', 1, 0)
    # Create a tag indicator for outdirection
    df_cleaned.loc[:, 'OUT'] = 0
    df_cleaned.loc[:, 'OUT'] = np.where(df_cleaned.loc[:, 'direction'] == 'out', 1, 0)
    # Tagging OUT duplicates
    df_cleaned = df_cleaned.sort_values(['eid', 'localtimestamp', 'direction'], ascending=False)  # .reset_index(drop=True)
    df_cleaned.loc[:, 'DUP_OUT'] = 0
    df_cleaned.loc[:, 'DUP_OUT'] = df_cleaned.groupby(['eid'])['OUT'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
    df_cleaned.head()
    df_cleaned = df_cleaned[df_cleaned.DUP_OUT <= 1]
    # Sorting in ascending order for cleaning of indirection
    df_cleaned = df_cleaned.sort_values(['eid', 'localtimestamp', 'direction'], ascending=True).reset_index(drop=True)
    df_cleaned.loc[:, 'DUP_IN'] = 0
    df_cleaned.loc[:, 'DUP_IN'] = df_cleaned.groupby(['eid'])['IN'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
    df_cleaned = df_cleaned[df_cleaned.DUP_IN <= 1]
    df_cleaned.drop(columns=['DUP_OUT', 'DUP_IN'], inplace=True)
    # Tagging records that are FIRST observation per EID
    df_cleaned['First_Tag'] = 0
    df_cleaned.loc[df_cleaned.groupby(['eid'], as_index=False).head(1).index, 'First_Tag'] = 1
    # Tagging records that are LAST observation per group
    df_cleaned['Last_Tag'] = 0
    df_cleaned.loc[df_cleaned.groupby(['eid'], as_index=False).tail(1).index, 'Last_Tag'] = 1
    ####DUP OUT PART 2
    df_cleaned.loc[:, 'DUP_OUT'] = 0
    df_cleaned.loc[:, 'DUP_OUT'] = df_cleaned.groupby(['eid'])['OUT'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
    df_cleaned.head()
    df_cleaned = df_cleaned[df_cleaned.DUP_OUT <= 1]

    return df_cleaned

def dft_less(table):
    
    df_cleaned = table
    df_orphan = df_cleaned[((df_cleaned.direction == "out") & (df_cleaned.First_Tag == 1)) | ((df_cleaned.direction == "in") & (df_cleaned.Last_Tag == 1))].copy()
    df_less = df_cleaned[~(((df_cleaned.direction == "out") & (df_cleaned.First_Tag == 1)) | ((df_cleaned.direction == "in") & (df_cleaned.Last_Tag == 1)))]
    # Drop unnecessary column
    df_less = df_less.drop(columns=['IN', 'OUT', 'First_Tag', 'Last_Tag', 'DateOnly'])
    df_orphan = df_orphan.drop(columns=['IN', 'OUT', 'First_Tag', 'Last_Tag', 'DateOnly'])
    # Store Previous Facility
    df_less = df_less.sort_values(['eid', 'localtimestamp', 'direction'], ascending=True).reset_index(drop=True)
    # Tagging records that are FIRST observation per EID
    df_less.loc[:, 'First_EID'] = 0
    df_less.loc[df_less.groupby(['eid'], as_index=False).head(1).index, 'First_EID'] = 1
    df_less.loc[:, 'Prev_Faci'] = df_less.loc[:, 'Facility'].shift()

    df_less.loc[:, 'Tag_Anomalous'] = np.where((df_less.loc[:, 'direction'] == 'out') & (df_less.loc[:, 'Facility'] != df_less['Prev_Faci']), 1, 0) 

    # IMPUTE Facility of outdirection WITH indirection
    df_less.loc[:, 'Facility'] = np.where((df_less.loc[:, 'direction'] == 'out') & \
                                        (df_less.loc[:, 'Facility'] != df_less['Prev_Faci']),
                                        df_less.loc[:, 'Prev_Faci'],
                                        df_less.loc[:, 'Facility']
                                        )
    df_less = df_less.drop(columns=['First_EID', 'Prev_Faci'])
    # tagging pairs of badges
    df_less.loc[:, 'Pair'] = df_less.groupby(['eid']).cumcount() + 1
    df_less.loc[:, 'Pair'] = np.where(df_less.loc[:, 'Pair'] % 2 == 0, df_less.loc[:, 'Pair'] - 1, df_less.loc[:, 'Pair'])
    # Duplicated rows
    df_less = df_less.drop_duplicates(['Facility', 'eid', 'Pair', 'direction'], keep=False)

    try:
        tz = df_less['localtimestamp'].dt.tz
        print(f"at df_less step the timezone status is {tz}")
    except Exception as e:
        print(f"Error getting timezone status at df_less step: {str(e)}")

    df_less = df_less[['Facility', 'eid', 'Pair', 'direction','localtimestamp','Tag_Anomalous', 'index_value', 'Group']]


    return df_less, df_orphan

def dft_wide1(table):
    
    df_less = table
    df_wide = df_less.set_index(['Facility', 'eid', 'Pair', 'direction']).unstack('direction').reset_index()
    df_wide.columns = df_wide.columns.to_series().str.join('_')
    df_wide.rename(columns={'Facility_': 'Facility', 'Pair_': 'Pair', \
                            'eid_': 'eid', \
                            'localtimestamp_in': 'time_in', \
                            'localtimestamp_out': 'time_out',  \
                            'Tag_Anomalous_out': 'Tag_Anomalous', \
                            'Group_in': 'Group', \
                            'index_value_in': 'index_value'}, inplace=True)

    df_wide = df_wide[['Facility', 'eid','Pair','time_in','time_out','Tag_Anomalous', 'index_value', 'Group']]
        
    return df_wide


def dft_wide2(table):
    
    df_wide = table
    # Remove Timezone
    try:
        df_wide['time_in'] = pd.to_datetime(df_wide['time_in']).dt.tz_localize(None)
        df_wide['time_out'] = pd.to_datetime(df_wide['time_out']).dt.tz_localize(None)
        print("Successfully remove timezone")
    except Exception as e:
        print(f"Error removing timezone: {str(e)}")

    return df_wide

def dft_wide3(table):

    df_wide = table
    # Make sure time in and out are formatted as datetime
    df_wide.loc[:, 'time_in'] = pd.to_datetime(df_wide.loc[:, 'time_in'], format="%Y-%m-%d %H:%M:%S")  # , errors='coerce')
    df_wide.loc[:, 'time_out'] = pd.to_datetime(df_wide.loc[:, 'time_out'], format="%Y-%m-%d %H:%M:%S")  # , errors='coerce')

    return df_wide


def dft_wide4(table):

    df_wide = table

    try:
        df_wide.loc[:, 'day_diff'] = (pd.to_numeric(df_wide.loc[:, 'time_out'].dt.dayofyear) - pd.to_numeric(df_wide.loc[:, 'time_in'].dt.dayofyear))
        print("Successfully perform substraction")
    except Exception as e:
        print(f"Error performing substraction: {str(e)}")

    df_wide.loc[:, 'duration_mins_v2'] = pd.to_numeric(
            ((df_wide.loc[:, 'time_out'] - df_wide.loc[:, 'time_in']).dt.total_seconds()) / 60).astype(int)

    df_wide.loc[:, 'duration_mins_v2'] = df_wide.loc[:, 'duration_mins_v2'].abs()

    df_wide["Tag_OverlyLongStay"] = np.where((df_wide.loc[:, 'duration_mins_v2'] >= 720), 1, 0)
    df_wide["Tag_Final"] = np.where((df_wide.loc[:, 'Tag_Anomalous'] == 1) | (df_wide.loc[:, 'Tag_OverlyLongStay'] == 1), 1, 0)
    
    return df_wide


def dft_wide5(table):

    df_wide = table
    # Split Badge pairs that cross dates
    df_split = df_wide[df_wide.day_diff > 1].copy()
    # save split into 2 files, one for ajusting time out, one for time in.
    df_in = df_wide[df_wide.day_diff == 1].copy()
    df_out = df_wide[df_wide.day_diff == 1].copy()
    # impute time in
    df_in.loc[:, 'time_in'] = pd.to_datetime(df_in.loc[:, 'time_out'].dt.date.apply(str) + ' 00:01:00', format="%Y-%m-%d %H:%M:%S")
    # impute time out
    df_out.loc[:, 'time_out'] = pd.to_datetime(df_out.loc[:, 'time_in'].dt.date.apply(str) + ' 23:59:00', format="%Y-%m-%d %H:%M:%S")
    # save same day badge ONLY
    df_wide = df_wide[df_wide.day_diff == 0]
    # Append imputed split days badges with same day
    df_wide = pd.concat([df_wide, df_in, df_out], axis=0, ignore_index=True)
    del df_in, df_out; gc.collect()
    # Compute hours of stay - duration_mins
    
    try:
        df_wide.loc[:, 'duration_mins'] = pd.to_numeric(((df_wide.loc[:, 'time_out'] - df_wide.loc[:, 'time_in']).dt.total_seconds()) / 60).astype(int)
        print("Successfully perform substraction")
    except Exception as e:
        print(f"Error performing substraction: {str(e)}")
    
    df_wide.loc[:, 'duration_mins'] = df_wide.loc[:, 'duration_mins'].abs()
    # Drop rows with duration_minss <=0
    df_wide = df_wide[df_wide.duration_mins > 0]  # drop0mins
    # drop columns not needed
    df_wide = df_wide.drop(columns=['Pair', 'day_diff'])

    return df_wide, df_split

def dft_wide_wSplitOrphan(table, split, orphan, month):
    
    df_wide = table
    df_split = split
    df_orphan = orphan

    df_orphan['Tag_Final'] = 1
    df_orphan['Tag_Anomalous'] = 1
    df_orphan['Tag_OverlyLongStay'] = 0
    
    try:
        # Split day more than 2 day
        df_split1 = df_split.copy()
        df_split2 = df_split.copy()
        del df_split; gc.collect()
        df_split1.loc[:, 'localtimestamp'] = df_split1.loc[:, 'time_in']
        df_split1.loc[:, 'direction'] = 'in'
        df_split1 = df_split1.drop(columns=['Pair', 'day_diff', 'time_in', 'time_out'])
        df_split2.loc[:, 'localtimestamp'] = df_split2.loc[:, 'time_out']
        df_split2.loc[:, 'direction'] = 'out'
        df_split2 = df_split2.drop(columns=['Pair', 'day_diff', 'time_in', 'time_out'])
        df_split2.sample()
        # Combine split with orphan
        df_orphan = pd.concat([df_orphan, df_split1, df_split2], axis=0, ignore_index=True)
        del df_split1, df_split2; gc.collect()
        # IMPUTE ORPHANED BADGES
        df_orphan.loc[:, 'time_in'] = np.where((df_orphan.loc[:, 'direction'] == "out"), \
                                                df_orphan.loc[:, 'localtimestamp'] - dt.timedelta(minutes=60), \
                                                df_orphan.loc[:, 'localtimestamp'])
        df_orphan.loc[:, 'time_out'] = np.where((df_orphan.loc[:, 'direction'] == "in"), \
                                                df_orphan.loc[:, 'localtimestamp'] + dt.timedelta(minutes=60), \
                                                df_orphan.loc[:, 'localtimestamp'])
        df_orphan.loc[:, 'duration_mins'] = 60
        df_orphan = df_orphan.drop(columns=['localtimestamp', 'direction'])

        df_orphan = df_orphan[['Facility', 'eid', 'time_in', 'time_out', 'Tag_Anomalous', 'Tag_Final', 'index_value','Tag_OverlyLongStay','duration_mins', 'Group']]

    except Exception as e:
        print(f'error {e}')
        pass
        
    df_wide = pd.concat([df_wide, df_orphan, ], axis=0, ignore_index=True)

    df_wide['time_in'] = pd.to_datetime(df_wide['time_in'], utc=True, errors='coerce').dt.tz_localize(None)
    df_wide.loc[:, 'Date'] = df_wide.loc[:, 'time_in'].dt.date
    df_wide.loc[:, 'badge'] = 1
    
    df_EID_daily = df_wide.groupby(['Facility', 'eid', 'Date'])[["duration_mins", 'badge']].sum().reset_index().copy()
    df_EID_daily.loc[:, 'duration_hrs'] = round((df_EID_daily.loc[:, 'duration_mins'] / 60), 2)

    df_EID_daily["Month"] = month

    return df_wide, df_EID_daily

def dft_gt(table):
    
    df_wide = table
    df_gt = df_wide[df_wide.duration_mins >= 30]

    print(df_gt['time_in'].apply(type).value_counts())

    # Ensure time_in and time_out are datetime and remove timezone
    try:
        df_gt['time_in'] = pd.to_datetime(df_gt['time_in'], utc=True, errors='coerce').dt.tz_localize(None)
        df_gt['time_out'] = pd.to_datetime(df_gt['time_out'], utc=True, errors='coerce').dt.tz_localize(None)
        print("Successfully ensured datetime format and removed timezone")
    except Exception as e:
        print(f"Error ensuring datetime and removing timezone: {str(e)}")
    
    df_gt['time_in'] = np.where((df_gt['time_in'].dt.minute) < 30, df_gt['time_in'].dt.floor('60T'), df_gt['time_in'].dt.ceil('60T'))
    df_gt['time_out'] = np.where((df_gt['time_out'].dt.minute) < 30, df_gt['time_out'].dt.floor('60T'), df_gt['time_out'].dt.ceil('60T'))
    df_gt.drop_duplicates(subset=['Facility', 'eid', 'time_in', 'time_out'], inplace=True)
    df_gt['row'] = range(len(df_gt))
    # reshape to df - every row two times repeated for each date of START_TIME and END_TIME
    starts = df_gt[['time_in', 'Facility', 'eid', 'Tag_Final', 'row','index_value', 'Group']].rename(columns={'time_in': 'Bin_time'})
    ends = df_gt[['time_out', 'Facility', 'eid', 'Tag_Final', 'row','index_value', 'Group']].rename(columns={'time_out': 'Bin_time'})
    df_gt = pd.concat([starts, ends])
    del starts
    del ends
    df_gt = df_gt.set_index('row', drop=True)
    df_gt = df_gt.sort_index()

    return df_gt

def dft_hourly(table, month):

    df_gt = table
    # resample and fill missing data
    df_hourly = df_gt.groupby(df_gt.index).resample('H', on='Bin_time').first()
    df_hourly = df_hourly.groupby(level=0).ffill()
    df_hourly = df_hourly.reset_index()
    df_hourly = df_hourly.drop(['row'], axis=1)
    df_hourly = df_hourly.drop_duplicates(['Facility', 'eid', 'Bin_time'], keep='first')
    df_hourly.loc[:, 'Date'] = df_hourly.loc[:, 'Bin_time'].dt.date
    df_hourly.loc[:, 'Bin_time'] = df_hourly.loc[:, 'Bin_time'].dt.time

    df_hourly_ex = df_hourly[df_hourly.Tag_Final==0].copy()
    df_hourly_ex = df_hourly_ex.rename(columns={'eid': 'eid_ex'})

    # Get number of unique EID by facility, floor, date, time
    df_hourly_count = df_hourly.groupby(['Facility', 'Date', 'Bin_time', 'Group'])['eid'].nunique().reset_index()
    # Create Time_group
    df_hourly_count.loc[:, 'Time_group'] = np.where(
        (df_hourly_count['Bin_time'] >= time(8, 0, 0)) & (df_hourly_count['Bin_time'] <= time(19, 0, 0)), '8AM-7PM',
        '8PM-7AM')

    df_hourly_count_ex = df_hourly_ex.groupby(['Facility', 'Date', 'Bin_time','Group'])['eid_ex'].nunique().reset_index()
    df_hourly_count = pd.merge(df_hourly_count, df_hourly_count_ex, on=['Facility', 'Date', 'Bin_time'], how='left')
    del df_hourly_ex, df_hourly_count_ex

    df_hourly_count["Month"] = month


    
    return df_hourly_count



def complete_peak_util_transformation(table, counter, grp_no, datalake_id, dataset_id, client, month, table_id_target4):
    
    group = counter
    # df_raw transformation
    try:
        df_raw = dft_raw(table)
        print(f"Successfully perform df_raw transformation for group {group}")
    except Exception as e:
        print(f"Error in df_raw transformation at group {group}: {str(e)}")
    


    # df_cleaned transformation
    try:
        df_cleaned = dft_cleaned(df_raw)
        del df_raw; gc.collect()
        print(f"Successfully perform df_cleaned transformation for group {group}")
    except Exception as e:
        print(f"Error in df_cleaned transformation at group {group}: {str(e)}")
    


    # df_less transformation
    try:
        df_less, df_orphan = dft_less(df_cleaned)
        del df_cleaned; gc.collect()
        print(f"Successfully perform df_less transformation for group {group}")
    except Exception as e:
        print(f"Error in df_less transformation at group {group}: {str(e)}")
    


    # df_wide1 transformation
    try:
        df_wide = dft_wide1(df_less)
        del df_less; gc.collect()
        print(f"Successfully perform df_wide1 transformation for group {group}")
    except Exception as e:
        print(f"Error in df_wide1 transformation at group {group}: {str(e)}") 
    

    # df_wide2 transformation
    try:
        df_wide = dft_wide2(df_wide)
        print(f"Successfully perform df_wide2 transformation for group {group}")
    except Exception as e:
        print(f"Error in df_wide2 transformation at group {group}: {str(e)}") 
    
    # df_wide3 transformation
    try:
        df_wide = dft_wide3(df_wide)
        print(f"Successfully perform df_wide3 transformation for group {group}")
    except Exception as e:
        print(f"Error in df_wide3 transformation at group {group}: {str(e)}") 
    


    # df_wide4 transformation
    try:
        df_wide = dft_wide4(df_wide)
        print(f"Successfully perform df_wide4 transformation for group {group}")
    except Exception as e:
        print(f"Error in df_wide4 transformation at group {group}: {str(e)}") 
    

    # df_wide5 transformation
    try:
        df_wide, df_split = dft_wide5(df_wide)
        print(f"Successfully perform df_wide5 transformation for group {group}")
    except Exception as e:
        print(f"Error in df_wide5 transformation at group {group}: {str(e)}")
    

    # df_wide with split and orphan transformation
    try:
        df_wide, df_EID_daily = dft_wide_wSplitOrphan(df_wide, df_split, df_orphan, month)
        del df_split, df_orphan; gc.collect()
        print(f"Successfully perform df_wide with split and orphan transformation for group {group}")
        
        write_to_bigquery_append(df_EID_daily, "df_EID_daily", datalake_id, dataset_id, table_id_target4, client, month, counter, grp_no)
    
    except Exception as e:
        print(f"Error in df_wide with split and orphan transformation at group {group}: {str(e)}")


 


    # df_gt transformation
    try:
        df_gt= dft_gt(df_wide)
        del df_wide, df_EID_daily; gc.collect()
        print(f"Successfully perform df_gt transformation for group {group}")
    except Exception as e:
        print(f"Error in df_gt transformation at group {group}: {str(e)}")
    

    # df_hourly transformation
    try:
        df_hourly_count= dft_hourly(df_gt, month)
        del df_gt; gc.collect()
        print(f"Successfully perform df_hourly transformation for group {group}")
    except Exception as e:
        print(f"Error in df_hourly transformation at group {group}: {str(e)}")

        
   
    

    return df_hourly_count










def transform_and_write_function(client, datalake_id, dataset_id, table_id_target1, table_id_target2, table_id_target4, query_job, batch_size, start_time, cb, month):
    
   
    buffer = []      
    counter = cb



    for row in query_job.result(page_size=batch_size):  # Stream rows in chunks
    
        row_dict = dict(row)
        row_dict["month"] = row_dict["month"].isoformat()


        #Time Check
        t = std_time.time() - start_time
        if t > 3300:
            Group = row_dict["Group"]
            return print(f"Group {Group} halted.")


        a = std_time.time()

        if row_dict["Group"]<=counter:
        
            buffer.append(row_dict)  # Convert row to dictionary
            grp_no = row_dict["Group"]

        else:
            
            df = pd.DataFrame(buffer) # Convert to data frame
            buffer.clear()  # Free memory
            buffer.append(row_dict) # Add the next group row
            
            df = complete_peak_util_transformation(df, counter, grp_no, datalake_id, dataset_id, client, month, table_id_target4) # Perform complete data transformation
            write_to_bigquery_append(df, "df_hourly_count", datalake_id, dataset_id, table_id_target2, client, month, counter, grp_no)
            del df; gc.collect()

            
            #write table tracker
            try:
                json_data = [{"current_batch": grp_no, "month": month}]
                write_to_bigquery(client, datalake_id, dataset_id, table_id_target1, json_data)
                print("Successfully write table tracker")
            except Exception as e:
                print(f"Error writing table tracker: {str(e)}")
 
            
            counter += 1

            b = std_time.time()
            duration = b - a
            print(f"The time it takes for group {grp_no} is {duration}")


            

    # Write remaining data if any
    if buffer:
        
        df = pd.DataFrame(buffer) # Convert to data frame
        buffer.clear()  # Free memory 
        
        df = complete_peak_util_transformation(df, counter, grp_no, datalake_id, dataset_id, client, month, table_id_target4) # Perform complete data transformation
        write_to_bigquery_append(df, "df_hourly_count", datalake_id, dataset_id, table_id_target2, client, month, counter, grp_no)
        del df; gc.collect()

        #write table tracker
        try:
            json_data = [{"current_batch": int(counter), "month": month}]
            write_to_bigquery(client, datalake_id, dataset_id, table_id_target1, json_data)
            print("Successfully write table tracker")
        except Exception as e:
            print(f"Error writing table tracker: {str(e)}")

        

    return print("Successfully write query in batches")
    


def SummarizedTable(datalake_id, dataset_id, table_id, client):

    table_id_target = "peak_util_table_summary"
    destination_table = f"{datalake_id}.{dataset_id}.{table_id_target}"

    sql = f"""
    SELECT *
    FROM `{datalake_id}.{dataset_id}.{table_id}`
    """

    df = gcp2df_(sql, client)
    df = df.groupby(['Facility', 'Date', 'Bin_time'], as_index=False)['eid', 'eid_ex'].sum()
    df = df.astype(str)
    json_data = df.to_dict(orient="records")
    client.load_table_from_json(json_data, destination_table).result()
    


@functions_framework.http
def call_destroy_azure(request):

    start_time = std_time.time() 

    
    batch = #{Batch_Number}#
    GroupSize = 1000000

    project_id = "#{GCP_PROJECT_ID}#"
    datalake_id= "#{GCP_DATALAKE_PROJECT_ID_PD}#" 
    dataset_id = "#{dataset_id}#" 
    table_id1 = "#{ccure_with_groupings_table}#"
    table_id2 = f"peak_util_tracker_{batch}"
    table_id3 = "batchlist"
    table_id4 = "daterange_table"
    table_id_target1 = f"peak_util_tracker_{batch}"
    table_id_target2 = "#{peak_utility_table}#"
    table_id_target3 = "batch_completed_PeakUtil"
    table_id_target4 = "#{df_EID_daily_table}#"

    location = "#{GCP_PROJECT_REGION}#"
    function_name = "#{TRIGGER_FUNCTION_NAME}#"
    job_id = f"prd-229817-{function_name}"
    job_name = f"projects/{project_id}/locations/{location}/jobs/{job_id}"
    
    target_table = f"{datalake_id}.{dataset_id}.{table_id_target3}"

    # Connect to client
    try:
        client = bigquery.Client(project=project_id)
        print("Successfully connected to project id client")
    except Exception as e:
        print(f"Error connecting to project id client: {str(e)}")


    # Create MonthList
    try:
        MonthList = GetTable(client, datalake_id, dataset_id, table_id4)['date'].tolist()
        MonthList = [date.strftime('%Y-%m-%d') for date in MonthList]
        print("Successfully created Monthlist")
    except Exception as e:
        print(f"Error creating Monthlist: {str(e)}")

    print(MonthList)


    # Create List of lists
    try:
        bl = CreateListOfLists(client, MonthList, batch, datalake_id, dataset_id, table_id3)
        print("Successfully created list of lists")
    except Exception as e:
        print(f"Error creating list of lists: {str(e)}")
    
    # Check progress status
    try:
        df_status = GetTable(client, datalake_id, dataset_id, table_id2)
        print("progress status available")
        df_status['month'] = df_status['month'].astype(str)
        cm = df_status.iloc[0]['month'] 
        cb = df_status.iloc[0]['current_batch']
        pos = MonthList.index(cm)

        if cb == bl[pos][-1] and cm == MonthList[-1]:

            table_ref = f"{datalake_id}.{dataset_id}.{table_id2}"
            client.delete_table(table_ref, not_found_ok=True)

            value = f"batch {batch} recorded"
            json_data = [{"Logs": value}]
            client.load_table_from_json(json_data, target_table).result()

            client_sched = scheduler_v1.CloudSchedulerClient()

            client_sched.pause_job(name=job_name)
            print("All rows has already beend process")
            return "All rows has already beend process"

        elif cb == bl[pos][-1]:
            pos +=1
            cb = bl[pos][0]
        else:
            cb += 1

        
        Range = list(range(len(MonthList))[pos:])

    except Exception as e:
        print(f"Error getting progress status: {str(e)}")
        Range = list(range(len(MonthList)))
        cb = bl[0][0]
        
    print(Range)

    for n in Range:
        month = MonthList[n]
        lb = bl[n][-1]

        print(f"now processing {month}")

        # Get Ccure Data
        try:
            query_job = GetCcureData(client, datalake_id, dataset_id, table_id1, month, cb, lb)
            print(f"Successfully get Ccure data for {month}")
        except Exception as e:
            print(f"Error getting Ccure data: {str(e)}")

        # Perform Transformation and write data
        try:
            transform_and_write_function(client, datalake_id, dataset_id, table_id_target1, table_id_target2, table_id_target4, query_job, GroupSize, start_time, cb, month)       
        except Exception as e:
            print(f"Error writing query in batches: {str(e)}")

        timea = std_time.time()

        t = std_time.time() - start_time
        if t > 3300:
            return f"Time out - execution halted"

        # Get the first element of the next list
        try:
            cb = bl[n+1][0] 
            print(f"Successfully get the first element of the next list: batch {cb}")
            print(cb)

        except Exception as e:
            print(f"Error getting the first element of the next list: {str(e)}")

                



    
    # Delete table
    try:
        table_ref = f"{datalake_id}.{dataset_id}.{table_id2}"
        client.delete_table(table_ref, not_found_ok=True)
        print("Successfully deleted peak_util_tracker table")
    except Exception as e:
        print(f"Error deleting peak_util_tracker table: {str(e)}")

    #Record Completion
    try:
        value = f"batch {batch} recorded"
        json_data = [{"Logs": value}]
        client.load_table_from_json(json_data, target_table).result()
        print("Successfully recorded batch completion")
    except Exception as e:
        print(f"Error recording batch completion: {str(e)}")
  
    # Connect to Scheduler Client
    try:
        client_sched = scheduler_v1.CloudSchedulerClient()
        print("Successfully connected to client_scheduler")
    except Exception as e:
        print(f"Error Connecting to client_scheduler: {str(e)}")

    # Halt scheduler job
    try:    
        client_sched.pause_job(name=job_name)
        print("Successfully pause scheduler job")
    except Exception as e:
        print(f"Error resuming scheduler job: {str(e)}")

    timeb = std_time.time()
    duration = timeb - timea

    print(f"duration from last loop to halt schedule: {duration}")


    end_time = std_time.time()

    elapsed_time = end_time - start_time

    print(f"PROCESSING TIME: {elapsed_time}")


    return f'Completed successfully'