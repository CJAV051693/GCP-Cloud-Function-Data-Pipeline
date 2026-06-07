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
from google.protobuf import field_mask_pb2



def gcp2df_(sql, client):
    query = client.query(sql)
    results = query.result()
    return results.to_dataframe()

def GetTable(client, datalake_id, dataset_id, table_id):

    sql = f"""
    SELECT *
    FROM `{datalake_id}.{dataset_id}.{table_id}`
    """
    df_status = gcp2df_(sql, client)
   
    return df_status

def Get_BatchInfo(client, datalake_id, dataset_id, table_id2, MonthList):    

    Record = []
    for month in MonthList:
        sql = f"""
        SELECT *
        FROM `{datalake_id}.{dataset_id}.{table_id2}`
        WHERE month = DATE('{month}')  
        ORDER BY `Group` DESC
        LIMIT 1
        """
        query_job = client.query(sql)  # Run query
        results = query_job.result()   # Fetch results
    
        for row in results:
            Record.append(dict(row))
    
    df = pd.DataFrame(Record)

    print(df)

    return df

def CreateBatchList(df):    

    df["batch1"] = np.maximum(np.where((df["Group"] % 5 == 0) | (df["Group"] % 4 == 0) , df["Group"]// 5, df["Group"]// 4)*1,1)
    df["batch2"] = np.maximum(np.where((df["Group"] % 5 == 0) | (df["Group"] % 4 == 0) , df["Group"]// 5, df["Group"]// 4)*2,2)
    df["batch3"] = np.maximum(np.where((df["Group"] % 5 == 0) | (df["Group"] % 4 == 0) , df["Group"]// 5, df["Group"]// 4)*3,3)
    df["batch4"] = np.maximum(np.where((df["Group"] % 5 == 0) | (df["Group"] % 4 == 0) , df["Group"]// 5, df["Group"]// 4)*4,4)
    df["batch5"] = np.maximum(df["Group"],5)

    df = df[["batch1", "batch2", "batch3", "batch4", "batch5", "month"]]

    return df


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



def clean_tables(client, project_id: str, dataset_id: str, table_id3: str, table_id4: str, table_id5: str, table_id6: str, frequency):
    """
    Deletes rows in target tables where Month matches any date in daterange_table
    or is older than 13 months from the latest date in daterange_table.
    """
    # Table references
    daterange_table = f"{project_id}.{dataset_id}.{table_id3}"
    target_tables = [
        f"{project_id}.{dataset_id}.{table_id4}",
        f"{project_id}.{dataset_id}.{table_id5}",
        f"{project_id}.{dataset_id}.{table_id6}"
    ]

    

    if frequency == "Weekly":
        # Loop through target tables and delete rows
        for table in target_tables:
            sql_delete = f"""
                DELETE FROM `{table}`
                WHERE Month IN (SELECT date FROM `{daterange_table}`)
                OR Month < DATE_SUB((SELECT MAX(date) FROM `{daterange_table}`), INTERVAL 13 MONTH)
            """
            query_job = client.query(sql_delete)
            query_job.result()  # Wait for completion
            print(f"Deleted rows in table: {table}")

    elif frequency == "Monthly":

        for table in target_tables:
            sql_delete = f"""
                DELETE FROM `{table}`
                WHERE Month < DATE_SUB((SELECT MAX(date) FROM `{daterange_table}`), INTERVAL 12 MONTH)
            """
            query_job = client.query(sql_delete)
            query_job.result()  # Wait for completion
            print(f"Deleted rows in table: {table}")

    else:
        print("No deletion happened")



@functions_framework.http
def call_destroy_azure(request):

    start_time = std_time.time()
    project_id = "#{GCP_PROJECT_ID}#"
    datalake_id= "#{GCP_DATALAKE_PROJECT_ID_PD}#" 
    dataset_id = "#{dataset_id}#" 
    table_id1 = "batch_completed_ccuregroupings"
    table_id2 = "#{ccure_with_groupings_table}#" 
    table_id3 = "daterange_table"
    table_id4 = "#{df_EID_daily_table}#" 
    table_id5 = "#{data_quality_table}#"
    table_id6 = "#{peak_utility_table}#"
    table_id_target1 = "batchlist"


    location = "#{GCP_PROJECT_REGION}#"


    job_id1 = "prd-229817-peak-util-batch1"
    job_id2 = "prd-229817-peak-util-batch2"
    job_id3 = "prd-229817-peak-util-batch3"
    job_id4 = "prd-229817-peak-util-batch4"
    job_id5 = "prd-229817-peak-util-batch5"
    job_id6 = "prd-229817-data-quality-batch1"
    job_id7 = "prd-229817-data-quality-batch2"
    job_id8 = "prd-229817-data-quality-batch3"
    job_id9 = "prd-229817-data-quality-batch4"
    job_id10 = "prd-229817-data-quality-batch5"
    job_id11 = "prd-229817-trigger-function1"
    job_id12 = "prd-229817-trigger-function2"
  
    job_name1 = f"projects/{project_id}/locations/{location}/jobs/{job_id1}"
    job_name2 = f"projects/{project_id}/locations/{location}/jobs/{job_id2}"
    job_name3 = f"projects/{project_id}/locations/{location}/jobs/{job_id3}"
    job_name4 = f"projects/{project_id}/locations/{location}/jobs/{job_id4}"
    job_name5 = f"projects/{project_id}/locations/{location}/jobs/{job_id5}"
    job_name6 = f"projects/{project_id}/locations/{location}/jobs/{job_id6}"
    job_name7 = f"projects/{project_id}/locations/{location}/jobs/{job_id7}"
    job_name8 = f"projects/{project_id}/locations/{location}/jobs/{job_id8}"
    job_name9 = f"projects/{project_id}/locations/{location}/jobs/{job_id9}"
    job_name10 = f"projects/{project_id}/locations/{location}/jobs/{job_id10}"
    job_name11 = f"projects/{project_id}/locations/{location}/jobs/{job_id11}"
    job_name12 = f"projects/{project_id}/locations/{location}/jobs/{job_id12}"

    timezone = ZoneInfo("Asia/Singapore")

    frequency = "#{frequency}#"


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


    # Create MonthList
    try:
        MonthList = GetTable(client, datalake_id, dataset_id, table_id3)['date'].tolist()
        MonthList = [date.strftime('%Y-%m-%d') for date in MonthList]
        print("Successfully created Monthlist")
    except Exception as e:
        print(f"Error creating Monthlist: {str(e)}")

    print(MonthList)
    

    df = None
    CompletedBatches = 0

 
    t = std_time.time() - start_time

    while t<3300:

        # Get the batch completion logs
        try:
            df = GetTable(client, datalake_id, dataset_id, table_id1)
            print("Successfully get batch completion logs")
        except Exception as e:
            print(f"Error getting batch completion logs: {str(e)}")

        # Get no. of completed batch
        try:
            CompletedBatches = len(df['Logs'].unique().tolist())
            print("Successfully get number of completed batches")
        except Exception as e:
            print(f"Error getting number of completed batches: {str(e)}")

        
        if CompletedBatches == 5:

            # Get Batch Info
            try:
                df = Get_BatchInfo(client, datalake_id, dataset_id, table_id2, MonthList)
                print(df)
                print("Successfully get batch info")
            except Exception as e:
                print(f"Error getting batch info: {str(e)}")
        

            # Create batch list
            try:
                df = CreateBatchList(df)
                print(df)
                print("Successfully create batch list")
            except Exception as e:
                print(f"Error creating batch list: {str(e)}")
        

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


            # Clean tables PeakUtil, DataQuality, dfEID

            try:
                clean_tables(client, datalake_id, dataset_id, table_id3, table_id4, table_id5, table_id6, frequency)
                print(f"Successfully clean the tables of PeakUtil, dfEID, DataQuality for {frequency} frequency")
            except Exception as e:
                print(f"Error in cleaning the tables of PeakUtil, dfEID, DataQuality: {str(e)}")
        

            # Get the current job configuration
            try:
                job1 = client_sched.get_job(name=job_name1)
                job2 = client_sched.get_job(name=job_name2)
                job3 = client_sched.get_job(name=job_name3)
                job4 = client_sched.get_job(name=job_name4)
                job5 = client_sched.get_job(name=job_name5)
                job6 = client_sched.get_job(name=job_name6)
                job7 = client_sched.get_job(name=job_name7)
                job8 = client_sched.get_job(name=job_name8)
                job9 = client_sched.get_job(name=job_name9)
                job10 = client_sched.get_job(name=job_name10)
                job12 = client_sched.get_job(name=job_name12)
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
                job7.schedule = new_cron_schedule
                job8.schedule = new_cron_schedule
                job9.schedule = new_cron_schedule
                job10.schedule = new_cron_schedule
                job12.schedule = new_cron_schedule
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
                client_sched.update_job(job=job7, update_mask=update_mask)
                client_sched.update_job(job=job8, update_mask=update_mask)
                client_sched.update_job(job=job9, update_mask=update_mask)
                client_sched.update_job(job=job10, update_mask=update_mask)
                client_sched.update_job(job=job12, update_mask=update_mask)
                print("Successfully update the job with the new cron schedule")
            except Exception as e:
                print(f"Error updating the job with the new cron schedule: {str(e)}")


            # Resume PeakUtil Scheduler
            try:    
                client_sched.resume_job(name=job_name1)
                client_sched.resume_job(name=job_name2)
                client_sched.resume_job(name=job_name3)
                client_sched.resume_job(name=job_name4)
                client_sched.resume_job(name=job_name5)
                print("Successfully resume peak util schedulers")
            except Exception as e:
                print(f"Error resuming peak util schedulers: {str(e)}")

            # Resume DataQuality Scheduler
            try:    
                client_sched.resume_job(name=job_name6)
                client_sched.resume_job(name=job_name7)
                client_sched.resume_job(name=job_name8)
                client_sched.resume_job(name=job_name9)
                client_sched.resume_job(name=job_name10)
                print("Successfully resume data quality schedulers")
            except Exception as e:
                print(f"Error resuming data quality schedulers: {str(e)}")


            # Resume trigger function2 scheduler job
            try:    
                client_sched.resume_job(name=job_name12)
                print("Successfully resume scheduler job3")
            except Exception as e:
                print(f"Error resuming scheduler job3: {str(e)}")

            # Halt trigger function1 scheduler job
            try:    
                client_sched.pause_job(name=job_name11)
                print("Successfully pause scheduler job4")
            except Exception as e:
                print(f"Error pausing scheduler job4: {str(e)}")

            return 'Completed successfully'
         
        else:
            pass

        std_time.sleep(300) 
        print("batch not yet completed")

        t = std_time.time() - start_time




    
    return 'Completed successfully'
