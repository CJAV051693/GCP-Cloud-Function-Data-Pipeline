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
    # GetBatchlist
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

    # Set Variables
    try:
        bl = []
        m = batchno
        n = m - 1
        bm = f"batch{m}"
        bn = f"batch{n}"
        print("Successfully set variables")
    except Exception as e:
        print(f"Error setting variables: {str(e)}")

    # Start looping for list of lists
    try:
        for month in MonthList:

            if m == 1:
                a = df.loc[df['month'] == month, bm].iloc[0]
                batch = list(range(1, a + 1))

            else:
                a = df.loc[df['month'] == month, bn].iloc[0]
                b = df.loc[df['month'] == month, bm].iloc[0]
                batch = list(range(a + 1, b + 1))

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



def write_to_bigquery_append(dt, step, datalake_id, dataset_id, table_id_target, client, month, counter, grp_no):

    destination_table = f"{datalake_id}.{dataset_id}.{table_id_target}"

    # Convert all NAs to zero
    try:
        dt = dt.replace({np.nan: 0})
        print("Successfully convert all NAs to zero")
    except Exception as e:
        print(f"Error converting all NAs to zero: {str(e)}")

    # Convert all columns to proper datatype
    try:
        dt = dt.replace({np.nan: 0})
        cols = dt.columns.difference(['Facility', 'Month'])
        dt[cols] = dt[cols].astype(int)
        dt[['Facility','Month']] = dt[['Facility','Month']].astype(str)
        print("Successfully convert all columns to proper data type")
    except Exception as e:
        print(f"Error converting all columns to proper data type: {str(e)}")


    # Convert dataframe to json format
    try:
        json_data = dt.to_dict(orient="records")
        print("Successfully convert dataframe to json")
    except Exception as e:
        print(f"Error converting dataframe to json: {str(e)}")

    # Write fully transformed data
    try:
        client.load_table_from_json(json_data, destination_table).result()
        del dt, json_data; gc.collect()
        print("Successfully write fully transformed table")
        print(month)
        print(counter)
        print(f"Group {counter} {step} written.")
        print(f"Group* {grp_no} {step} written.")
    except Exception as e:
        print(f"Error writing Group {counter} {step} table: {str(e)}")




def dft_raw(table):
    df_raw = table
    df_raw.dropna(how="all", inplace=True)
    df_raw.localtimestamp = pd.to_datetime(df_raw.localtimestamp, format="%Y-%m-%d %H:%M:%S", errors='coerce')
    df_raw['LocalTime'] = df_raw['localtimestamp'].dt.strftime("%H:%M:%S")
    df_raw['DateOnly'] = df_raw['localtimestamp'].dt.strftime("%Y-%m-%d")
    df_raw.drop_duplicates(inplace=True)
    df_raw.loc[:, 'door'] = df_raw.loc[:, 'door'].str.replace(" ", "")
    df_raw['Facility'] = (df_raw.door.str[0:11:1]).str.upper()
    df_raw.loc[:, 'eid'] = df_raw.loc[:, 'eid'].str.lower()
    df_raw.loc[:, 'eid'] = df_raw.loc[:, 'eid'].str.replace(" ", "")
    df_raw.dropna(subset=['eid'], inplace=True)
    df_raw.dropna(subset=['door'], inplace=True)
    df_raw.loc[:, 'direction'] = df_raw.loc[:, 'direction'].str.lower()
    df_raw.loc[:, 'direction'] = df_raw.loc[:, 'direction'].str.replace(" ", "")
    df_raw.dropna(subset=['direction'], inplace=True)
    df_raw.drop(columns=['messagetype'], inplace=True)
    df_raw.drop(columns=['door'], inplace=True)
    df_raw.loc[:, 'DateOnly'] = df_raw.loc[:, 'localtimestamp'].dt.date
    df_raw.sample(2)
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
    df_cleaned.loc[:, 'IN'] = 0
    df_cleaned.loc[:, 'IN'] = np.where(df_cleaned.loc[:, 'direction'] == 'in', 1, 0)
    df_cleaned.loc[:, 'OUT'] = 0
    df_cleaned.loc[:, 'OUT'] = np.where(df_cleaned.loc[:, 'direction'] == 'out', 1, 0)
    df_cleaned = df_cleaned.sort_values(['eid', 'localtimestamp', 'direction'],
                                        ascending=False)  # .reset_index(drop=True)
    df_cleaned.loc[:, 'DUP_OUT'] = 0
    df_cleaned.loc[:, 'DUP_OUT'] = df_cleaned.groupby(['eid'])['OUT'] \
        .transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
    df_cleaned = df_cleaned[df_cleaned.DUP_OUT <= 1]
    df_cleaned = df_cleaned.sort_values(['eid', 'localtimestamp', 'direction'], ascending=True).reset_index(
        drop=True)
    df_cleaned.loc[:, 'DUP_IN'] = 0
    df_cleaned.loc[:, 'DUP_IN'] = df_cleaned.groupby(['eid'])['IN'] \
        .transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
    df_cleaned = df_cleaned[df_cleaned.DUP_IN <= 1]
    df_cleaned.drop(columns=['DUP_OUT', 'DUP_IN'], inplace=True)
    df_cleaned['First_Tag'] = 0
    df_cleaned.loc[df_cleaned.groupby(['eid'], as_index=False).head(1).index, 'First_Tag'] = 1
    df_cleaned['Last_Tag'] = 0
    df_cleaned.loc[df_cleaned.groupby(['eid'], as_index=False).tail(1).index, 'Last_Tag'] = 1
    df_cleaned.loc[:, 'DUP_OUT'] = 0
    df_cleaned.loc[:, 'DUP_OUT'] = df_cleaned.groupby(['eid'])['OUT'] \
        .transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
    df_cleaned = df_cleaned[df_cleaned.DUP_OUT <= 1]

    return df_cleaned


def dft_less(table):
    df_cleaned = table
    df_orphan = df_cleaned[((df_cleaned.direction == "out") & (df_cleaned.First_Tag == 1)) | (
                (df_cleaned.direction == "in") & (df_cleaned.Last_Tag == 1))].copy()
    df_less = df_cleaned[~(((df_cleaned.direction == "out") & (df_cleaned.First_Tag == 1)) | (
                (df_cleaned.direction == "in") & (df_cleaned.Last_Tag == 1)))]
    df_less = df_less.drop(columns=['IN', 'OUT', 'First_Tag', 'Last_Tag', 'DateOnly'])
    df_orphan = df_orphan.drop(columns=['IN', 'OUT', 'First_Tag', 'Last_Tag', 'DateOnly'])

    grouped_df_orphan = df_orphan.groupby(['Facility']).size().reset_index(name='No of orphan badges')

    df_less = df_less.sort_values(['eid', 'localtimestamp', 'direction'], ascending=True).reset_index(drop=True)
    df_less.loc[:, 'First_EID'] = 0
    df_less.loc[df_less.groupby(['eid'], as_index=False).head(1).index, 'First_EID'] = 1
    df_less.loc[:, 'Prev_Faci'] = df_less.loc[:, 'Facility'].shift()
    df_less.loc[:, 'Tag_Anomalous'] = np.where(
        (df_less.loc[:, 'direction'] == 'out') & (df_less.loc[:, 'Facility'] != df_less['Prev_Faci']), 1, 0)
    # grouped_df_less_Anomalous_BeforeFloorImputation = df_less.groupby(['Facility']).agg(Anomalous_count=('Tag_Anomalous', lambda x: (x == 1).sum()), total_count=('Tag_Anomalous', 'count')).reset_index().copy()
    df_less.loc[:, 'Original Facility'] = df_less.loc[:, 'Facility']
    df_less.loc[:, 'Facility'] = np.where(
        (df_less.loc[:, 'direction'] == 'out') & (df_less.loc[:, 'Facility'] != df_less['Prev_Faci']),
        df_less.loc[:, 'Prev_Faci'], df_less.loc[:, 'Facility'])

    grouped_df_less_Anomalous_AfterFloorImputation = df_less.groupby(['Facility','Group']).agg(
        Anomalous_count=('Tag_Anomalous', lambda x: (x == 1).sum()),
        total_count=('Tag_Anomalous', 'count')).reset_index().copy()
    grouped_df_less_Anomalous_AfterFloorImputation = grouped_df_less_Anomalous_AfterFloorImputation.rename(
        columns={'Anomalous_count': 'TotalPairsOfAnomalousBadges_AfterFloorImputation',
                 'total_count': 'TotalBadges_AfterFloorImputation'})

    df_less = df_less.drop(columns=['First_EID', 'Prev_Faci'])
    df_less.loc[:, 'Pair'] = df_less.groupby(['eid']).cumcount() + 1
    df_less.loc[:, 'Pair'] = np.where(df_less.loc[:, 'Pair'] % 2 == 0, df_less.loc[:, 'Pair'] - 1,
                                      df_less.loc[:, 'Pair'])
    df_less = df_less.drop_duplicates(['Facility', 'Original Facility', 'eid', 'Pair', 'direction', 'Tag_Anomalous'],
                                      keep=False)

    df_less = df_less[['Facility', 'eid', 'Pair', 'direction','localtimestamp','Tag_Anomalous', 'index_value','Group']]

    return df_less, grouped_df_orphan, grouped_df_less_Anomalous_AfterFloorImputation


def dft_wide(df_less, grouped_df_orphan, grouped_df_less_Anomalous_AfterFloorImputation, month):
    df_wide = df_less.set_index(['Facility', 'eid', 'Pair', 'direction']).unstack('direction').reset_index()
    df_wide.columns = df_wide.columns.to_series().str.join('_')
    df_wide.rename(columns={'Facility_': 'Facility', 'Pair_': 'Pair', \
                            'eid_': 'eid', \
                            'localtimestamp_in': 'time_in', \
                            'localtimestamp_out': 'time_out', \
                            'Tag_Anomalous_out': 'Tag_Anomalous', \
                            'Group_in': 'Group', \
                            'index_value_in': 'index_value'}, inplace = True)

    df_wide = df_wide[['Facility', 'eid', 'Pair', 'time_in', 'time_out', 'Tag_Anomalous', 'index_value', 'Group']]

    # Remove Timezone
    try:
        df_wide['time_in'] = pd.to_datetime(df_wide['time_in']).dt.tz_localize(None)
        df_wide['time_out'] = pd.to_datetime(df_wide['time_out']).dt.tz_localize(None)
        print("Successfully remove timezone")
    except Exception as e:
        print(f"Error removing timezone: {str(e)}")

    # Make sure time in and out are formatted as datetime
    df_wide.loc[:, 'time_in'] = pd.to_datetime(df_wide.loc[:, 'time_in'],
                                               format="%Y-%m-%d %H:%M:%S")  # , errors='coerce')
    df_wide.loc[:, 'time_out'] = pd.to_datetime(df_wide.loc[:, 'time_out'],
                                                format="%Y-%m-%d %H:%M:%S")  # , errors='coerce')

    try:
        df_wide.loc[:, 'day_diff'] = (pd.to_numeric(df_wide.loc[:, 'time_out'].dt.dayofyear) - pd.to_numeric(
            df_wide.loc[:, 'time_in'].dt.dayofyear))
        print("Successfully perform substraction")
    except Exception as e:
        print(f"Error performiing substraction: {str(e)}")

    grouped_df_wide_Allpairs = df_wide.groupby(['Facility']).size().reset_index(name='All pairs of badges').copy()

    try:
        df_wide.loc[:, 'duration_mins'] = pd.to_numeric(
            ((df_wide.loc[:, 'time_out'] - df_wide.loc[:, 'time_in']).dt.total_seconds()) / 60).astype(int)
        print("Successfully perform substraction")
    except Exception as e:
        print(f"Error performing substraction: {str(e)}")

    df_wide.loc[:, 'duration_mins'] = df_wide.loc[:, 'duration_mins'].abs()
    # Drop rows with duration_minss <=0
    df_wide = df_wide[df_wide.duration_mins > 0]  # drop0mins
    # drop columns not needed
    df_wide = df_wide.drop(columns=['Pair', 'day_diff'])

    df_wide_30 = df_wide[(df_wide.duration_mins == 30) & (df_wide["Tag_Anomalous"] == 0)].copy()
    df_wide_30_anomalous = df_wide[(df_wide.duration_mins == 30) & (df_wide["Tag_Anomalous"] == 1)].copy()

    grouped_df_wide_30 = df_wide_30.groupby(['Facility']).size().reset_index(
        name='30minsPairsOfBadges_NotAnomalous')
    grouped_df_wide_30_anomalous = df_wide_30_anomalous.groupby(['Facility']).size().reset_index(
        name='30minsPairsOfBadges_Anomalous')

    # Create Summary
    DataProfilingSummary = pd.merge(grouped_df_less_Anomalous_AfterFloorImputation, grouped_df_orphan, on='Facility',
                                    how='outer')
    del grouped_df_less_Anomalous_AfterFloorImputation, grouped_df_orphan;
    gc.collect()
    DataProfilingSummary = pd.merge(DataProfilingSummary, grouped_df_wide_Allpairs, on='Facility', how='left')
    del grouped_df_wide_Allpairs;
    gc.collect()
    DataProfilingSummary = pd.merge(DataProfilingSummary, grouped_df_wide_30, on='Facility', how='left')
    del grouped_df_wide_30;
    gc.collect()
    DataProfilingSummary = pd.merge(DataProfilingSummary, grouped_df_wide_30_anomalous, on='Facility', how='left')
    del grouped_df_wide_30_anomalous;
    gc.collect()

    # Store results in dictionaries instead of globals
    df_by_hour = {}
    grouped_by_hour = {}
    df_by_hour_anomalous = {}
    grouped_by_hour_anomalous = {}

    for hour in range(8, 49):
        start_min = hour * 60
        end_min = (hour + 1) * 60

        df_by_hour[hour] = df_wide[(df_wide.duration_mins >= start_min) & (df_wide.duration_mins < end_min) & (
                    df_wide["Tag_Anomalous"] == 0)].copy()
        df_by_hour_anomalous[hour] = df_wide[
            (df_wide.duration_mins >= start_min) & (df_wide.duration_mins < end_min) & (
                        df_wide["Tag_Anomalous"] == 1)].copy()

        grouped_by_hour[hour] = df_by_hour[hour].groupby(['Facility']).size().reset_index(
            name=f'{hour}hrTo{hour + 1}hrPairsOfBadges_NotAnomalous')
        grouped_by_hour_anomalous[hour] = df_by_hour_anomalous[hour].groupby(['Facility']).size().reset_index(
            name=f'{hour}hrTo{hour + 1}hrPairsOfBadges_Anomalous')

        del df_by_hour[hour];
        gc.collect()
        del df_by_hour_anomalous[hour];
        gc.collect()

        DataProfilingSummary = pd.merge(DataProfilingSummary, grouped_by_hour[hour], on='Facility', how='left')
        DataProfilingSummary = pd.merge(DataProfilingSummary, grouped_by_hour_anomalous[hour], on='Facility',
                                        how='left')

        del grouped_by_hour[hour];
        gc.collect()
        del grouped_by_hour_anomalous[hour];
        gc.collect()

    df_wide_49hrAbove = df_wide[(df_wide.duration_mins >= 2940) & (df_wide["Tag_Anomalous"] == 0)].copy()
    df_wide_49hrAbove_anomalous = df_wide[(df_wide.duration_mins >= 2940) & (df_wide["Tag_Anomalous"] == 1)].copy()
    del df_wide;
    gc.collect()

    grouped_df_wide_49hrAbove = df_wide_49hrAbove.groupby(['Facility']).size().reset_index(
        name='MoreThan49HourPairsOfBadges_NotAnomalous')
    del df_wide_49hrAbove;
    gc.collect()

    grouped_df_wide_49hrAbove_anomalous = df_wide_49hrAbove_anomalous.groupby(['Facility']).size().reset_index(
        name='MoreThan49HourPairsOfBadges_Anomalous')
    del df_wide_49hrAbove_anomalous;
    gc.collect()

    DataProfilingSummary = pd.merge(DataProfilingSummary, grouped_df_wide_49hrAbove, on='Facility', how='left')
    del grouped_df_wide_49hrAbove;
    gc.collect()
    DataProfilingSummary = pd.merge(DataProfilingSummary, grouped_df_wide_49hrAbove_anomalous, on='Facility',
                                    how='left')
    del grouped_df_wide_49hrAbove_anomalous;
    gc.collect()
    DataProfilingSummary["Month"] = month

    return DataProfilingSummary


def complete_data_quality_transformation(df, counter, month):
    group = counter
    # df_raw transformation
    try:
        df_raw = dft_raw(df)
        print(f"Successfully perform df_raw transformation for group {group}")
    except Exception as e:
        print(f"Error in df_raw transformation at group {group}: {str(e)}")

    # df_cleaned transformation
    try:
        df_cleaned = dft_cleaned(df_raw)
        del df_raw;
        gc.collect()
        print(f"Successfully perform df_cleaned transformation for group {group}")
    except Exception as e:
        print(f"Error in df_cleaned transformation at group {group}: {str(e)}")

    # df_less transformation
    try:
        df_less, grouped_df_orphan, grouped_df_less_Anomalous_AfterFloorImputation = dft_less(df_cleaned)
        del df_cleaned;
        gc.collect()
        print(f"Successfully perform df_less transformation for group {group}")
    except Exception as e:
        print(f"Error in df_less transformation at group {group}: {str(e)}")

    # df_wide transformation
    try:
        dt = dft_wide(df_less, grouped_df_orphan, grouped_df_less_Anomalous_AfterFloorImputation, month)
        del df_less, grouped_df_orphan, grouped_df_less_Anomalous_AfterFloorImputation;
        gc.collect()
        print(f"Successfully perform df_wide transformation for group {group}")
    except Exception as e:
        print(f"Error in df_wide transformation at group {group}: {str(e)}")

    return dt


def transform_and_write_function(client, datalake_id, dataset_id, table_id_target1, table_id_target2, query_job,
                                 batch_size, start_time, cb, month):
    buffer = []
    counter = cb

    destination_table = f"{datalake_id}.{dataset_id}.{table_id_target2}"

    for row in query_job.result(page_size=batch_size):  # Stream rows in chunks

        row_dict = dict(row)
        row_dict["month"] = row_dict["month"].isoformat()

        # Time Check
        t = std_time.time() - start_time
        if t > 3300:
            Group = row_dict["Group"]
            return print(f"Group {Group} halted.")


        a = std_time.time()

        if row_dict["Group"] <= counter:

            buffer.append(row_dict)  # Convert row to dictionary
            grp_no = row_dict["Group"]

        else:

            df = pd.DataFrame(buffer)  # Convert to data frame
            buffer.clear()  # Free memory
            buffer.append(row_dict)  # Add the next group row

            df = complete_data_quality_transformation(df, counter, month)  # Perform complete data transformation
            write_to_bigquery_append(df, "data quality last step", datalake_id, dataset_id, table_id_target2, client, month, counter, grp_no)
            del df; gc.collect()

            # write table tracker
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

        df = pd.DataFrame(buffer)  # Convert to data frame
        buffer.clear()  # Free memory

        df = complete_data_quality_transformation(df, counter, month)  # Perform complete data transformation
        write_to_bigquery_append(df, "data quality last step", datalake_id, dataset_id, table_id_target2, client, month, counter, grp_no)
        del df; gc.collect()

        #write table tracker
        try:
            json_data = [{"current_batch": int(counter), "month": month}]
            write_to_bigquery(client, datalake_id, dataset_id, table_id_target1, json_data)
            print("Successfully write table tracker")
        except Exception as e:
            print(f"Error writing table tracker: {str(e)}")

        

    return print("Successfully write query in batches")

@functions_framework.http
def call_destroy_azure(request):
    start_time = std_time.time()

    batch = #{Batch_Number}#
    GroupSize = 1000000

    project_id = "#{GCP_PROJECT_ID}#"
    datalake_id= "#{GCP_DATALAKE_PROJECT_ID_PD}#" 
    dataset_id = "#{dataset_id}#"
    table_id1 = "#{ccure_with_groupings_table}#"
    table_id2 = f"data_quality_tracker_{batch}"
    table_id3 = "batchlist"
    table_id4 = "daterange_table"
    table_id_target1 = f"data_quality_tracker_{batch}"
    table_id_target2 = "#{data_quality_table}#"
    table_id_target3 = "batch_completed_DataQuality"

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
            print("All rows has already been process")
            return "All rows has already been process"

        elif cb == bl[pos][-1]:
            pos += 1
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
            transform_and_write_function(client, datalake_id, dataset_id, table_id_target1, table_id_target2, query_job,
                                         GroupSize, start_time, cb, month)
        except Exception as e:
            print(f"Error writing query in batches: {str(e)}")

        timea = std_time.time()

        t = std_time.time() - start_time
        if t > 3300:
            return f"Time out - execution halted"

        # Get the first element of the next list
        try:
            cb = bl[n + 1][0]
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

    # Record Completion
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