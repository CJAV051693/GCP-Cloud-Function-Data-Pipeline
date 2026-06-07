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


def CreateListOfLists(client, MonthList, batchno, datalake_id, dataset_id, table_id4):    

    #GetBatchlist
    try:
        sql = f"""
        SELECT *
        FROM `{datalake_id}.{dataset_id}.{table_id4}`
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


def JoinOperation(client, datalake_id, dataset_id, table_id1, table_id2, table_id3, month, cb, scope):

    s = scope[-1]


    print(f"filtering batches between {cb} and {s}")

    sql = f"""
    SELECT  ccure.* EXCEPT(rank), 
            eid.Group, 
            map.Door as door
    FROM `{datalake_id}.{dataset_id}.{table_id1}` ccure
    LEFT JOIN `{datalake_id}.{dataset_id}.{table_id2}` eid 
        ON ccure.eid = eid.eid AND eid.month = DATE('{month}')
    LEFT JOIN `{datalake_id}.{dataset_id}.ccure_door_mapping` map
        ON ccure.rank = map.rank
    WHERE ccure.month = DATE('{month}')
        AND ccure.index_value >= {cb} 
        AND ccure.index_value <= {s}
    ORDER BY ccure.index_value ASC
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

def write_batch_to_bigquery_(client, datalake_id, dataset_id, table_id_target1, table_id_target2, query_job, batch_size, start_time):
    
   
    buffer = []      
    batch_count = None
    month = None


    destination_table = f"{datalake_id}.{dataset_id}.{table_id_target2}"

    for row in query_job.result(page_size=batch_size):  # Stream rows in chunks
    
        row_dict = dict(row)
        row_dict["month"] = row_dict["month"].isoformat()

        buffer.append(row_dict)  # Convert row to dictionary

        #Time Check

        t = std_time.time() - start_time
        if t > 3300:
            batch_count = row_dict["index_value"]
            return print(f"Batch {batch_count} halted.")
        

        # When batch is full, write to BigQuery and clear buffer
        if len(buffer) >= batch_size:

            timea = std_time.time()


            # Load batch to table
            try:
                client.load_table_from_json(buffer, destination_table).result()
                buffer.clear()  # Free memory
                
                month = row_dict["month"]
                batch_count = row_dict["index_value"]
                print(month)
                print(batch_count)
                print(f"Batch {batch_count} written.")

            except Exception as e:
                print(f"Error loading batch to table: {str(e)}")

            #write table tracker
            try:
                json_data = [{"current_batch": batch_count, "month": month}]
                write_to_bigquery(client, datalake_id, dataset_id, table_id_target1, json_data)
                print("Successfully write table tracker")
            except Exception as e:
                print(f"Error writing table tracker: {str(e)}")

            timeb = std_time.time()
            duration = timeb - timea

            print(f"Duration: {duration}")


        
        month = row_dict["month"]
        batch_count = row_dict["index_value"]
          

    # Write remaining data if any
    if buffer:
        # Load batch to table
        try:
            client.load_table_from_json(buffer, destination_table).result()
            buffer.clear()  # Free memory
            
            month = row_dict["month"]
            batch_count = row_dict["index_value"]
            print(month)
            print(batch_count)
            print(f"Batch {batch_count} written.")

        except Exception as e:
            print(f"Error loading batch to table: {str(e)}")

        #write table tracker
        try:
            json_data = [{"current_batch": batch_count, "month": month}]
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
    table_id1 = "#{table_id}#"
    table_id2 = "eidgroupings"
    table_id3 = f"ccure_groupings_tracker_{batch}"
    table_id4 = "batchlist"
    table_id5 = "daterange_table"
    table_id_target1 = f"ccure_groupings_tracker_{batch}"
    table_id_target2 = "ccure_with_groupings"
    table_id_target3 = "batch_completed_ccuregroupings"

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
        raise 

    # Create MonthList
    try:
        MonthList = GetTable(client, datalake_id, dataset_id, table_id5)['date'].tolist()
        MonthList = [date.strftime('%Y-%m-%d') for date in MonthList]
        print("Successfully created Monthlist")
    except Exception as e:
        print(f"Error creating Monthlist: {str(e)}")

    print(MonthList)


    # Create List of lists
    try:
        bl = CreateListOfLists(client, MonthList, batch, datalake_id, dataset_id, table_id4)
        print("Successfully created list of lists")
    except Exception as e:
        print(f"Error creating list of lists: {str(e)}")
    
    # Check progress status
    try:
        df_status = GetTable(client, datalake_id, dataset_id, table_id3)
        print("progress status available")
        df_status['month'] = df_status['month'].astype(str)
        cm = df_status.iloc[0]['month'] 
        cb = df_status.iloc[0]['current_batch']
        pos = MonthList.index(cm)

        if cb == bl[pos][-1] and cm == MonthList[-1]:

            table_ref = f"{datalake_id}.{dataset_id}.{table_id3}"
            client.delete_table(table_ref, not_found_ok=True)

            value = f"batch {batch} recorded"
            json_data = [{"Logs": value}]
            client.load_table_from_json(json_data, target_table).result()

            client_sched = scheduler_v1.CloudSchedulerClient()

            client_sched.pause_job(name=job_name)

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
        scope = bl[n]

        print(f"now processing {month}")

        # Perform Join Operation
        try:
            query_job = JoinOperation(client, datalake_id, dataset_id, table_id1, table_id2, table_id3, month, cb, scope)
            print("Successfully Perform Join Operation")
        except Exception as e:
            print(f"Error creating Performing Joing Operation: {str(e)}")

        # Write query in batches
        try:
            write_batch_to_bigquery_(client, datalake_id, dataset_id, table_id_target1, table_id_target2, query_job, GroupSize, start_time)       
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
        table_ref = f"{datalake_id}.{dataset_id}.{table_id3}"
        client.delete_table(table_ref, not_found_ok=True)
        print("Successfully deleted ccure grouping tacker table")
    except Exception as e:
        print(f"Error deleting ccure grouping tracker table: {str(e)}")

    

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