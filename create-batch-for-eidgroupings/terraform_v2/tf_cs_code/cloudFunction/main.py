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
from google.cloud import scheduler_v1
from zoneinfo import ZoneInfo
from google.protobuf import field_mask_pb2


def gcp2df_(sql, client):
    query = client.query(sql)
    results = query.result()
    return results.to_dataframe()




def Get_BatchInfo(client, datalake_id, dataset_id, table_id, MonthList):    

    Record = []
    for month in MonthList:
        sql = f"""
        SELECT *
        FROM `{datalake_id}.{dataset_id}.{table_id}`
        WHERE month = DATE('{month}')  
        ORDER BY index_value DESC
        LIMIT 1
        """
        query_job = client.query(sql)  # Run query
        results = query_job.result()   # Fetch results
    
        for row in results:
            Record.append(dict(row))
    
    df = pd.DataFrame(Record)

    return df

def CreateBatchList(df):    

    df["batch1"] = np.maximum(np.where((df["index_value"] % 5 == 0) | (df["index_value"] % 4 == 0), df["index_value"]// 5, df["index_value"]// 4)*1,1)
    df["batch2"] = np.maximum(np.where((df["index_value"] % 5 == 0) | (df["index_value"] % 4 == 0), df["index_value"]// 5, df["index_value"]// 4)*2,2)
    df["batch3"] = np.maximum(np.where((df["index_value"] % 5 == 0) | (df["index_value"] % 4 == 0), df["index_value"]// 5, df["index_value"]// 4)*3,3)
    df["batch4"] = np.maximum(np.where((df["index_value"] % 5 == 0) | (df["index_value"] % 4 == 0), df["index_value"]// 5, df["index_value"]// 4)*4,4)
    df["batch5"] = np.maximum(df["index_value"],5)

    df = df[["batch1", "batch2", "batch3", "batch4", "batch5", "month"]]

    return df

def GROUP(client, MonthList, GroupSize, datalake_id, dataset_id, table_id):

    list_df = []

    for month in MonthList:
        
        query = f"""
        SELECT 
            eid,
            month,
            COUNT(*) AS count
        FROM `{datalake_id}.{dataset_id}.{table_id}`
        WHERE 
            month = DATE('{month}')
        GROUP BY 
            eid,
            month
        """
        Eid = gcp2df_(query, client)[['eid', 'count', 'month']]  # Load only necessary columns
        Eid = Eid.sort_values(by=['count', 'eid'], ascending=[True, True]).reset_index(drop=True)  # Sorting (optional)

        # Convert "count" column to a NumPy array for efficiency
        counts = Eid["count"].to_numpy()

        cumulative_sums = np.zeros(len(counts), dtype=np.int32)  # Use int32 instead of Python int
        group_list = np.zeros(len(counts), dtype=np.int16)  # Use int16 for minimal memory usage

        cumulative_sum = 0
        group = 1

        for i in range(len(counts)):
            if cumulative_sum + counts[i] > GroupSize:
                cumulative_sum = counts[i]  # Restart the sum
                group += 1  # Increment group
            else:
                cumulative_sum += counts[i]

            cumulative_sums[i] = cumulative_sum
            group_list[i] = group

        # Assign optimized NumPy arrays back to the DataFrame
        Eid["Group"] = group_list.astype(np.int16)  # Reduce memory usage
        Eid["Cumulative_Sum"] = cumulative_sums.astype(np.int32)
        
        Eid = Eid[['eid','count','Cumulative_Sum', 'Group','month']] 

        del counts, cumulative_sums, group_list
        gc.collect()
        
        list_df.append(Eid)

    Eid = pd.concat(list_df, axis=0, ignore_index=True)

    print(Eid)

    return Eid


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

def get_month_list(dateinput: str, datalake_id: str, dataset_id: str, table_id: str, client, frequency, today):

 

    if dateinput.strip():  # case when dateinput is not empty
            months = [d.strip() for d in dateinput.split(",")]
    else:
        if frequency == "Weekly":

            days_ago = today - dt.timedelta(days=10)
            
            months = {
                today.replace(day=1).strftime("%Y-%m-%d"),
                days_ago.replace(day=1).strftime("%Y-%m-%d")
            }
            months = sorted(list(months))
                
            print(f"the month list is {months}")

            # --- Check today's month in BigQuery ---
            today_month = dt.date.today().replace(day=1).strftime("%Y-%m-%d")
            print(f"today month is {today_month}")

            sql = f"""
                SELECT *
                FROM `{datalake_id}.{dataset_id}.{table_id}`
                WHERE month = DATE('{today_month}')
                LIMIT 1
            """

            query_job = client.query(sql)
            results = list(query_job.result())

            # If no rows, drop today's month
            if not results:

                months = [m for m in months if m != today_month]
                print("query table is empty")
        else:
            first_day_this_month = today.replace(day=1)
            last_day_prev_month = first_day_this_month - dt.timedelta(days=1)
            prev_month = last_day_prev_month.replace(day=1).strftime("%Y-%m-%d")
            months = [prev_month]
            
            
    return months




@functions_framework.http
def call_destroy_azure(request):
    
    GroupSize = 250000

    project_id = "#{GCP_PROJECT_ID}#"
    datalake_id= "#{GCP_DATALAKE_PROJECT_ID_PD}#" 
    dataset_id = "#{dataset_id}#" 
    table_id = "#{table_id}#"
    table_id_target1 = "batchlist"
    table_id_target2 = "eidgroupings"
    table_id_target3 = "daterange_table"

 
    dateinput = "#{Date_Range}#"
    today_test = "#{today_test}#" #Format "2025-08-25"
    ORIGINAL_CRON = "#{gcp_cs_schedule}#"

    location = "#{GCP_PROJECT_REGION}#"
    job_id1 = "prd-229817-create-ccure-with-groupings-batch1"
    job_id2 = "prd-229817-create-ccure-with-groupings-batch2"
    job_id3 = "prd-229817-create-ccure-with-groupings-batch3"
    job_id4 = "prd-229817-create-ccure-with-groupings-batch4"
    job_id5 = "prd-229817-create-ccure-with-groupings-batch5"
    job_id6 = "prd-229817-trigger-function1"
    job_id7 = "prd-229817-create-batch-for-eidgroupings"

    job_name1 = f"projects/{project_id}/locations/{location}/jobs/{job_id1}"
    job_name2 = f"projects/{project_id}/locations/{location}/jobs/{job_id2}"
    job_name3 = f"projects/{project_id}/locations/{location}/jobs/{job_id3}"
    job_name4 = f"projects/{project_id}/locations/{location}/jobs/{job_id4}"
    job_name5 = f"projects/{project_id}/locations/{location}/jobs/{job_id5}"
    job_name6 = f"projects/{project_id}/locations/{location}/jobs/{job_id6}"
    job_name7 = f"projects/{project_id}/locations/{location}/jobs/{job_id7}"

    timezone = ZoneInfo("Asia/Singapore")
    
    frequency = "#{frequency}#"


    #Set Today variable
    if today_test.strip():
        # Non-empty → parse it
        today = dt.date.fromisoformat(today_test)
    else:
        # Empty → use today's date
        today = dt.datetime.now(timezone).date()

    weekday = today.weekday()  # 0=Mon, 6=Sun  

    # Connect to client
    try:
        client = bigquery.Client(project=project_id)
        print("Successfully connected to project id client")
    except Exception as e:
        print(f"Error connecting to project id client: {str(e)}")
        raise
    

    # Connect to Scheduler Client
    try:
        client_sched = scheduler_v1.CloudSchedulerClient()
        print("Successfully connected to client_scheduler")
    except Exception as e:
        print(f"Error Connecting to client_scheduler: {str(e)}")

    job7 = client_sched.get_job(name=job_name7)

    if weekday in (5, 6):  # Saturday=5, Sunday=6
        # Compute next Monday
        days_to_monday = (7 - weekday) % 7
        next_monday = today + dt.timedelta(days=days_to_monday)
        new_day = next_monday.day
        new_cron = f"0 0 {new_day} * *"
        
        print(f"Weekend detected. Updating cron to run on Monday {next_monday} → {new_cron}")
        job7.schedule = new_cron
        update_mask = {"paths": ["schedule"]}
        client_sched.update_job(job=job7, update_mask=update_mask)

        return f"Schedule updated to next Monday ({next_monday})"

    #Start Main Operation 


    # Get MonthList
    try:
        MonthList = get_month_list(dateinput, datalake_id, dataset_id, table_id, client, frequency, today)
        print("Successfully get monthlist")
        print(MonthList)
    except Exception as e:
        print(f"Error getting monthlist: {str(e)}")
        raise  


    # Get Batch Info
    try:
        df = Get_BatchInfo(client, datalake_id, dataset_id, table_id, MonthList)
        print("Successfully get batch info")
    except Exception as e:
        print(f"Error getting batch info: {str(e)}")
        raise 

    # Create batch list
    try:
        df = CreateBatchList(df)
        print("Successfully create batch list")
    except Exception as e:
        print(f"Error creating batch list: {str(e)}")
        raise 

    # Convert all columns of batchlist dataframe to string format
    try:
        df = df.astype(str)
        print("Successfully convert all columns to string for batchlist")
    except Exception as e:
        print(f"Error converting all columns to string for batchlist: {str(e)}")


    # Convert batchlist dataframe to json format
    try:
        json_data = df.to_dict(orient="records")
        print("Successfully convert dataframe to json")
    except Exception as e:
        print(f"Error converting dataframe to json: {str(e)}")

    # Write batchlist to BigQuery
    try:    
        write_to_bigquery(client, datalake_id, dataset_id, table_id_target1, json_data)
        print("Successfully write data")
    except Exception as e:
        print(f"Error writing data: {str(e)}")


    # Create EID groupings
    try:
        Eid = GROUP(client, MonthList, GroupSize, datalake_id, dataset_id, table_id)
        print("Successfully created EID groupings")
    except Exception as e:
        print(f"Error creating EID groupings: {str(e)}")
    

    # Convert all columns of Eid dataframe to string format
    try:
        Eid = Eid.astype(str)
        print("Successfully convert all columns to string for EID Groupings")
    except Exception as e:
        print(f"Error converting all columns to string for EID Groupings: {str(e)}")

    # Convert Eid groupings dataframe to json format
    try:
        json_data = Eid.to_dict(orient="records")
        print("Successfully convert dataframe to json")
    except Exception as e:
        print(f"Error converting dataframe to json: {str(e)}")

    # Write Eid groupings to BigQuery
    try:    
        write_to_bigquery(client, datalake_id, dataset_id, table_id_target2, json_data)
        print("Successfully write data")
    except Exception as e:
        print(f"Error writing data: {str(e)}")


    # Create daterange dataframe
    try:
        datetable = pd.DataFrame(MonthList, columns=['date'])
        print("Successfully created daterange dataframe")
    except Exception as e:
        print(f"Error creating daterange dataframe: {str(e)}")

    # Convert all columns of daterange dataframe to string format
    try:
        datetable = datetable.astype(str)
        print("Successfully convert all columns to string for daterange dataframe")
    except Exception as e:
        print(f"Error converting all columns to string for daterange dataframe: {str(e)}")

    # Convert daterange dataframe to json format
    try:
        json_data = datetable.to_dict(orient="records")
        print("Successfully convert daterange dataframe to json")
    except Exception as e:
        print(f"Error converting daterange dataframe to json: {str(e)}")

    # Write daterange to BigQuery
    try:    
        write_to_bigquery(client, datalake_id, dataset_id, table_id_target3, json_data)
        print("Successfully write daterange to bigquery")
    except Exception as e:
        print(f"Error writing daterange to bigquery: {str(e)}")

    # Get the current job configuration
    try:
        job1 = client_sched.get_job(name=job_name1)
        job2 = client_sched.get_job(name=job_name2)
        job3 = client_sched.get_job(name=job_name3)
        job4 = client_sched.get_job(name=job_name4)
        job5 = client_sched.get_job(name=job_name5)
        job6 = client_sched.get_job(name=job_name6)
        print("Successfully get the current jobs configuration")
    except Exception as e:
        print(f"Error getting the current jobs configuration: {str(e)}")


     # Update cron schedule
    try:
        current_time = dt.datetime.now(timezone)
        future_time = current_time + dt.timedelta(minutes=5)
        minute = future_time.minute
        new_cron_schedule = f"{minute} * * * *"
        print("successfully update cron schedule")
    except Exception as e:
        print(f"Error updating cron schedule: {str(e)}")


    # Update the schedule field
    try:
        job1.schedule = new_cron_schedule
        job2.schedule = new_cron_schedule
        job3.schedule = new_cron_schedule
        job4.schedule = new_cron_schedule
        job5.schedule = new_cron_schedule
        job6.schedule = new_cron_schedule
        print("Successfully Update the schedule field for all jobs")
    except Exception as e:
        print(f"Error updating the schedule field for all jobs: {str(e)}")

    # Prepare a field mask for updating only the schedule
    try:
        update_mask = field_mask_pb2.FieldMask(paths=["schedule"])
        print("Successfully prepare a field mask for updating only the schedule")
    except Exception as e:
        print(f"Error preparing a field mask for updating only the schedule: {str(e)}")

    # Update the job with the new cron schedule
    try:
        client_sched.update_job(job=job1, update_mask=update_mask)
        client_sched.update_job(job=job2, update_mask=update_mask)
        client_sched.update_job(job=job3, update_mask=update_mask)
        client_sched.update_job(job=job4, update_mask=update_mask)
        client_sched.update_job(job=job5, update_mask=update_mask)
        client_sched.update_job(job=job6, update_mask=update_mask)
        print("Successfully update the job with the new cron schedule")
    except Exception as e:
        print(f"Error updating the job with the new cron schedule: {str(e)}")



    # Try resume scheduler jobs for create-ccure-with-groupings
    try:    
        client_sched.resume_job(name=job_name1)
        client_sched.resume_job(name=job_name2)
        client_sched.resume_job(name=job_name3)
        client_sched.resume_job(name=job_name4)
        client_sched.resume_job(name=job_name5)
        client_sched.resume_job(name=job_name6)

        print("Successfully resume scheduler jobs for create-ccure-with-groupings ")
    except Exception as e:
        print(f"Error resuming scheduler jobs for create-ccure-with-groupings: {str(e)}")

    #End Main Operation



    current_cron = job7.schedule
    current_day = current_cron.split()[2]  # cron format: min hour day month do
    original_day = ORIGINAL_CRON.split()[2]

    if current_day != original_day:

        job7.schedule = ORIGINAL_CRON
        update_mask = {"paths": ["schedule"]}
        client_sched.update_job(job=job7, update_mask=update_mask)

        print(f"Cron reset to original: {ORIGINAL_CRON}")
    else:
        print(f"Cron already set to day {original_day}. No reset needed.")
    


    return f'Completed successfully'