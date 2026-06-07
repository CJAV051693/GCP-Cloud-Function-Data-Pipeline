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

def GetBatchCompletionLogs(client, datalake_id, dataset_id, table_id1):

    sql = f"""
    SELECT *
    FROM `{datalake_id}.{dataset_id}.{table_id1}`
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

    df["batch1"] = np.where(df["Group"] % 5 == 0, df["Group"]// 5, df["Group"]// 4)*1
    df["batch2"] = np.where(df["Group"] % 5 == 0, df["Group"]// 5, df["Group"]// 4)*2
    df["batch3"] = np.where(df["Group"] % 5 == 0, df["Group"]// 5, df["Group"]// 4)*3
    df["batch4"] = np.where(df["Group"] % 5 == 0, df["Group"]// 5, df["Group"]// 4)*4
    df["batch5"] = df["Group"]

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


def clean_tables(client, datalake_id: str, dataset_id: str, table_id3: str, table_id4: str, table_id5: str, frequency):
    """
    Deletes rows in target tables where Month matches any date in daterange_table
    or is older than 13 months from the latest date in daterange_table.
    """
    # Table references
    daterange_table = f"{datalake_id}.{dataset_id}.{table_id3}"
    target_tables = [
        f"{datalake_id}.{dataset_id}.{table_id4}",
        f"{datalake_id}.{dataset_id}.{table_id5}"
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

def SummarizedDataQuality(client, datalake_id, dataset_id, table_id1, table_id2, table_id3):

    daterange_table = f"{datalake_id}.{dataset_id}.{table_id1}"

    sql = f"""
    SELECT
        Facility,
        Month,
        SUM(MoreThan49HourPairsOfBadges_NotAnomalous) AS MoreThan49HourPairsOfBadges_NotAnomalous,
        SUM(`48hrTo49hrPairsOfBadges_NotAnomalous`) AS `48hrTo49hrPairsOfBadges_NotAnomalous`,
        SUM(`46hrTo47hrPairsOfBadges_Anomalous`) AS `46hrTo47hrPairsOfBadges_Anomalous`,
        SUM(`46hrTo47hrPairsOfBadges_NotAnomalous`) AS `46hrTo47hrPairsOfBadges_NotAnomalous`,
        SUM(`45hrTo46hrPairsOfBadges_NotAnomalous`) AS `45hrTo46hrPairsOfBadges_NotAnomalous`,
        SUM(`44hrTo45hrPairsOfBadges_Anomalous`) AS `44hrTo45hrPairsOfBadges_Anomalous`,
        SUM(`45hrTo46hrPairsOfBadges_Anomalous`) AS `45hrTo46hrPairsOfBadges_Anomalous`,
        SUM(`39hrTo40hrPairsOfBadges_Anomalous`) AS `39hrTo40hrPairsOfBadges_Anomalous`,
        SUM(`42hrTo43hrPairsOfBadges_NotAnomalous`) AS `42hrTo43hrPairsOfBadges_NotAnomalous`,
        SUM(`41hrTo42hrPairsOfBadges_Anomalous`) AS `41hrTo42hrPairsOfBadges_Anomalous`,
        SUM(`39hrTo40hrPairsOfBadges_NotAnomalous`) AS `39hrTo40hrPairsOfBadges_NotAnomalous`,
        SUM(`37hrTo38hrPairsOfBadges_Anomalous`) AS `37hrTo38hrPairsOfBadges_Anomalous`,
        SUM(`37hrTo38hrPairsOfBadges_NotAnomalous`) AS `37hrTo38hrPairsOfBadges_NotAnomalous`,
        SUM(`38hrTo39hrPairsOfBadges_Anomalous`) AS `38hrTo39hrPairsOfBadges_Anomalous`,
        SUM(`35hrTo36hrPairsOfBadges_Anomalous`) AS `35hrTo36hrPairsOfBadges_Anomalous`,
        SUM(`33hrTo34hrPairsOfBadges_Anomalous`) AS `33hrTo34hrPairsOfBadges_Anomalous`,
        SUM(`40hrTo41hrPairsOfBadges_Anomalous`) AS `40hrTo41hrPairsOfBadges_Anomalous`,
        SUM(`36hrTo37hrPairsOfBadges_NotAnomalous`) AS `36hrTo37hrPairsOfBadges_NotAnomalous`,
        SUM(`31hrTo32hrPairsOfBadges_Anomalous`) AS `31hrTo32hrPairsOfBadges_Anomalous`,
        SUM(`31hrTo32hrPairsOfBadges_NotAnomalous`) AS `31hrTo32hrPairsOfBadges_NotAnomalous`,
        SUM(`30hrTo31hrPairsOfBadges_Anomalous`) AS `30hrTo31hrPairsOfBadges_Anomalous`,
        SUM(`28hrTo29hrPairsOfBadges_NotAnomalous`) AS `28hrTo29hrPairsOfBadges_NotAnomalous`,
        SUM(`27hrTo28hrPairsOfBadges_NotAnomalous`) AS `27hrTo28hrPairsOfBadges_NotAnomalous`,
        SUM(`38hrTo39hrPairsOfBadges_NotAnomalous`) AS `38hrTo39hrPairsOfBadges_NotAnomalous`,
        SUM(`25hrTo26hrPairsOfBadges_Anomalous`) AS `25hrTo26hrPairsOfBadges_Anomalous`,
        SUM(`26hrTo27hrPairsOfBadges_Anomalous`) AS `26hrTo27hrPairsOfBadges_Anomalous`,
        SUM(`30hrTo31hrPairsOfBadges_NotAnomalous`) AS `30hrTo31hrPairsOfBadges_NotAnomalous`,
        SUM(`25hrTo26hrPairsOfBadges_NotAnomalous`) AS `25hrTo26hrPairsOfBadges_NotAnomalous`,
        SUM(`9hrTo10hrPairsOfBadges_Anomalous`) AS `9hrTo10hrPairsOfBadges_Anomalous`,
        SUM(`24hrTo25hrPairsOfBadges_NotAnomalous`) AS `24hrTo25hrPairsOfBadges_NotAnomalous`,
        SUM(`23hrTo24hrPairsOfBadges_NotAnomalous`) AS `23hrTo24hrPairsOfBadges_NotAnomalous`,
        SUM(`28hrTo29hrPairsOfBadges_Anomalous`) AS `28hrTo29hrPairsOfBadges_Anomalous`,
        SUM(`22hrTo23hrPairsOfBadges_Anomalous`) AS `22hrTo23hrPairsOfBadges_Anomalous`,
        SUM(`21hrTo22hrPairsOfBadges_Anomalous`) AS `21hrTo22hrPairsOfBadges_Anomalous`,
        SUM(`44hrTo45hrPairsOfBadges_NotAnomalous`) AS `44hrTo45hrPairsOfBadges_NotAnomalous`,
        SUM(`43hrTo44hrPairsOfBadges_Anomalous`) AS `43hrTo44hrPairsOfBadges_Anomalous`,
        SUM(`19hrTo20hrPairsOfBadges_NotAnomalous`) AS `19hrTo20hrPairsOfBadges_NotAnomalous`,
        SUM(`17hrTo18hrPairsOfBadges_NotAnomalous`) AS `17hrTo18hrPairsOfBadges_NotAnomalous`,
        SUM(MoreThan49HourPairsOfBadges_Anomalous) AS MoreThan49HourPairsOfBadges_Anomalous,
        SUM(`16hrTo17hrPairsOfBadges_Anomalous`) AS `16hrTo17hrPairsOfBadges_Anomalous`,
        SUM(`20hrTo21hrPairsOfBadges_NotAnomalous`) AS `20hrTo21hrPairsOfBadges_NotAnomalous`,
        SUM(`47hrTo48hrPairsOfBadges_NotAnomalous`) AS `47hrTo48hrPairsOfBadges_NotAnomalous`,
        SUM(`34hrTo35hrPairsOfBadges_Anomalous`) AS `34hrTo35hrPairsOfBadges_Anomalous`,
        SUM(`29hrTo30hrPairsOfBadges_Anomalous`) AS `29hrTo30hrPairsOfBadges_Anomalous`,
        SUM(`16hrTo17hrPairsOfBadges_NotAnomalous`) AS `16hrTo17hrPairsOfBadges_NotAnomalous`,
        SUM(`24hrTo25hrPairsOfBadges_Anomalous`) AS `24hrTo25hrPairsOfBadges_Anomalous`,
        SUM(`15hrTo16hrPairsOfBadges_Anomalous`) AS `15hrTo16hrPairsOfBadges_Anomalous`,
        SUM(`43hrTo44hrPairsOfBadges_NotAnomalous`) AS `43hrTo44hrPairsOfBadges_NotAnomalous`,
        SUM(`8hrTo9hrPairsOfBadges_Anomalous`) AS `8hrTo9hrPairsOfBadges_Anomalous`,
        SUM(`15hrTo16hrPairsOfBadges_NotAnomalous`) AS `15hrTo16hrPairsOfBadges_NotAnomalous`,
        SUM(`12hrTo13hrPairsOfBadges_NotAnomalous`) AS `12hrTo13hrPairsOfBadges_NotAnomalous`,
        SUM(`22hrTo23hrPairsOfBadges_NotAnomalous`) AS `22hrTo23hrPairsOfBadges_NotAnomalous`,
        SUM(`33hrTo34hrPairsOfBadges_NotAnomalous`) AS `33hrTo34hrPairsOfBadges_NotAnomalous`,
        SUM(`14hrTo15hrPairsOfBadges_Anomalous`) AS `14hrTo15hrPairsOfBadges_Anomalous`,
        SUM(`19hrTo20hrPairsOfBadges_Anomalous`) AS `19hrTo20hrPairsOfBadges_Anomalous`,
        SUM(`42hrTo43hrPairsOfBadges_Anomalous`) AS `42hrTo43hrPairsOfBadges_Anomalous`,
        SUM(`11hrTo12hrPairsOfBadges_NotAnomalous`) AS `11hrTo12hrPairsOfBadges_NotAnomalous`,
        SUM(`11hrTo12hrPairsOfBadges_Anomalous`) AS `11hrTo12hrPairsOfBadges_Anomalous`,
        SUM(`35hrTo36hrPairsOfBadges_NotAnomalous`) AS `35hrTo36hrPairsOfBadges_NotAnomalous`,
        SUM(`29hrTo30hrPairsOfBadges_NotAnomalous`) AS `29hrTo30hrPairsOfBadges_NotAnomalous`,
        SUM(`18hrTo19hrPairsOfBadges_Anomalous`) AS `18hrTo19hrPairsOfBadges_Anomalous`,
        SUM(`13hrTo14hrPairsOfBadges_Anomalous`) AS `13hrTo14hrPairsOfBadges_Anomalous`,
        SUM(`47hrTo48hrPairsOfBadges_Anomalous`) AS `47hrTo48hrPairsOfBadges_Anomalous`,
        SUM(`41hrTo42hrPairsOfBadges_NotAnomalous`) AS `41hrTo42hrPairsOfBadges_NotAnomalous`,
        SUM(`9hrTo10hrPairsOfBadges_NotAnomalous`) AS `9hrTo10hrPairsOfBadges_NotAnomalous`,
        SUM(TotalPairsOfAnomalousBadges_AfterFloorImputation) AS TotalPairsOfAnomalousBadges_AfterFloorImputation,
        SUM(`14hrTo15hrPairsOfBadges_NotAnomalous`) AS `14hrTo15hrPairsOfBadges_NotAnomalous`,
        SUM(`30minsPairsOfBadges_Anomalous`) AS `30minsPairsOfBadges_Anomalous`,
        SUM(`30minsPairsOfBadges_NotAnomalous`) AS `30minsPairsOfBadges_NotAnomalous`,
        SUM(`All pairs of badges`) AS `All pairs of badges`,
        SUM(`32hrTo33hrPairsOfBadges_NotAnomalous`) AS `32hrTo33hrPairsOfBadges_NotAnomalous`,
        SUM(`20hrTo21hrPairsOfBadges_Anomalous`) AS `20hrTo21hrPairsOfBadges_Anomalous`,
        SUM(`10hrTo11hrPairsOfBadges_NotAnomalous`) AS `10hrTo11hrPairsOfBadges_NotAnomalous`,
        SUM(`21hrTo22hrPairsOfBadges_NotAnomalous`) AS `21hrTo22hrPairsOfBadges_NotAnomalous`,
        SUM(`36hrTo37hrPairsOfBadges_Anomalous`) AS `36hrTo37hrPairsOfBadges_Anomalous`,
        SUM(`32hrTo33hrPairsOfBadges_Anomalous`) AS `32hrTo33hrPairsOfBadges_Anomalous`,
        SUM(`12hrTo13hrPairsOfBadges_Anomalous`) AS `12hrTo13hrPairsOfBadges_Anomalous`,
        SUM(`10hrTo11hrPairsOfBadges_Anomalous`) AS `10hrTo11hrPairsOfBadges_Anomalous`,
        SUM(`23hrTo24hrPairsOfBadges_Anomalous`) AS `23hrTo24hrPairsOfBadges_Anomalous`,
        SUM(`48hrTo49hrPairsOfBadges_Anomalous`) AS `48hrTo49hrPairsOfBadges_Anomalous`,
        SUM(`40hrTo41hrPairsOfBadges_NotAnomalous`) AS `40hrTo41hrPairsOfBadges_NotAnomalous`,
        SUM(`27hrTo28hrPairsOfBadges_Anomalous`) AS `27hrTo28hrPairsOfBadges_Anomalous`,
        SUM(`No of orphan badges`) AS `No of orphan badges`,
        SUM(`18hrTo19hrPairsOfBadges_NotAnomalous`) AS `18hrTo19hrPairsOfBadges_NotAnomalous`,
        SUM(`13hrTo14hrPairsOfBadges_NotAnomalous`) AS `13hrTo14hrPairsOfBadges_NotAnomalous`,
        SUM(TotalBadges_AfterFloorImputation) AS TotalBadges_AfterFloorImputation,
        SUM(`26hrTo27hrPairsOfBadges_NotAnomalous`) AS `26hrTo27hrPairsOfBadges_NotAnomalous`,
        SUM(`34hrTo35hrPairsOfBadges_NotAnomalous`) AS `34hrTo35hrPairsOfBadges_NotAnomalous`,
        SUM(`17hrTo18hrPairsOfBadges_Anomalous`) AS `17hrTo18hrPairsOfBadges_Anomalous`,
        SUM(`8hrTo9hrPairsOfBadges_NotAnomalous`) AS `8hrTo9hrPairsOfBadges_NotAnomalous`
    FROM `{datalake_id}.{dataset_id}.{table_id2}`
    WHERE Month IN (SELECT `date` FROM `{daterange_table}`)
    GROUP BY Facility, Month

    """
    destination_table = f"{datalake_id}.{dataset_id}.{table_id3}"

    # Configure the job
    try:
        job_config = bigquery.QueryJobConfig(
            destination=destination_table,
            write_disposition="WRITE_APPEND"  # options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        )
        print("succesfully set job_config")
    except Exception as e:
        print(f"Error setting jon_config: {str(e)}")


    # Run the query
    try:
        query_job = client.query(sql, job_config=job_config)
        print("Successfully run summarized query")
    except Exception as e:
        print(f"Error running summarized query: {str(e)}")

    # Wait for job to complete
    query_job.result()

    print(f"Query results successfully written to {destination_table}")

def SummarizedPeakUtility(client, datalake_id, dataset_id, table_id1, table_id2, table_id3):

    daterange_table = f"{datalake_id}.{dataset_id}.{table_id1}"

    sql = f"""
    SELECT
        Facility,
        Month,
        Bin_time,
        Time_group,
        `Date`,
        SUM(eid_ex) AS eid_ex,
        SUM(eid) AS eid,
        
    FROM `{datalake_id}.{dataset_id}.{table_id2}`
    WHERE Month IN (SELECT `date` FROM `{daterange_table}`)
    GROUP BY Facility, Month, Bin_time, Time_group, `Date`
    """
    destination_table = f"{datalake_id}.{dataset_id}.{table_id3}"

    # Configure the job
    try:
        job_config = bigquery.QueryJobConfig(
            destination=destination_table,
            write_disposition="WRITE_APPEND"  # options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        )
        print("succesfully set job_config")
    except Exception as e:
        print(f"Error setting jon_config: {str(e)}")


    # Run the query
    try:
        query_job = client.query(sql, job_config=job_config)
        print("Successfully run summarized query")
    except Exception as e:
        print(f"Error running summarized query: {str(e)}")
        
    # Wait for job to complete
    query_job.result()

    print(f"Query results successfully written to {destination_table}")




@functions_framework.http
def call_destroy_azure(request):

    start_time = std_time.time()
    project_id = "#{GCP_PROJECT_ID}#"
    datalake_id= "#{GCP_DATALAKE_PROJECT_ID_PD}#" 
    dataset_id = "#{dataset_id}#" 

    
    
    table_id1 = "batch_completed_DataQuality"
    table_id2 = "batch_completed_PeakUtil"
    table_id3 = "batch_completed_ccuregroupings"
    table_id4 = "batchlist"
    table_id5 = "#{ccure_with_groupings_table}#" 
    table_id6 = "eidgroupings"
    table_id7 = "daterange_table"

    table_id8 = "#{data_quality_table}#" 
    table_id9 = "#{peak_utility_table}#" 
    table_id10 = "#{data_quality_table}#_summary"
    table_id11 = "#{peak_utility_table}#_summary"
    
    location = "#{GCP_PROJECT_REGION}#"

    job_id1 = "prd-229817-trigger-function2"
    job_name1 = f"projects/{project_id}/locations/{location}/jobs/{job_id1}"

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

    df = None
    CompletedBatches_DataQuality = 0
    CompletedBatches_PeakUtil = 0

 
    t = std_time.time() - start_time

    while t<3300:

        # Get the batch completion logs for data quality
        try:
            df = GetBatchCompletionLogs(client, datalake_id, dataset_id, table_id1)
            print("Successfully get batch completion logs")
        except Exception as e:
            print(f"Error getting batch completion logs: {str(e)}")

        # Get no. of completed batch for data quality
        try:
            CompletedBatches_DataQuality = len(df['Logs'].unique().tolist())
            print("Successfully get number of completed batches")
        except Exception as e:
            print(f"Error getting number of completed batches: {str(e)}")

        # Get the batch completion logs for Peak Util
        try:
            df = GetBatchCompletionLogs(client, datalake_id, dataset_id, table_id2)
            print("Successfully get batch completion logs")
        except Exception as e:
            print(f"Error getting batch completion logs: {str(e)}")

        # Get no. of completed batch for Peak Util
        try:
            CompletedBatches_PeakUtil = len(df['Logs'].unique().tolist())
            print("Successfully get number of completed batches")
        except Exception as e:
            print(f"Error getting number of completed batches: {str(e)}")


        
        if CompletedBatches_DataQuality == 5 and CompletedBatches_PeakUtil == 5:

            # Clean tables PeakUtil summary, DataQuality summary  
            try:
                clean_tables(client, datalake_id, dataset_id, table_id7, table_id10, table_id11, frequency)
                print(f"Successfully clean the tables of PeakUtil, DataQuality summary tables {frequency} frequency")
            except Exception as e:
                print(f"Error in cleaning the tables of PeakUtil, DataQuality summary tables: {str(e)}")


            # Summarized tables PeakUtil, DataQuality

            try:
                SummarizedDataQuality(client, datalake_id, dataset_id, table_id7, table_id8, table_id10)
                SummarizedPeakUtility(client, datalake_id, dataset_id, table_id7, table_id9, table_id11)
                print("Successfully summarized PeakUtil, DataQuality tables")
            except Exception as e:
                print(f"Error in summarizing PeakUtil, DataQuality tables: {str(e)}")

            # Delete unused table

            try:
                table_ref1 = f"{datalake_id}.{dataset_id}.{table_id1}"
                table_ref2 = f"{datalake_id}.{dataset_id}.{table_id2}"
                table_ref3 = f"{datalake_id}.{dataset_id}.{table_id3}"
                table_ref4 = f"{datalake_id}.{dataset_id}.{table_id4}"
                table_ref5 = f"{datalake_id}.{dataset_id}.{table_id5}"
                table_ref6 = f"{datalake_id}.{dataset_id}.{table_id6}"
                table_ref7 = f"{datalake_id}.{dataset_id}.{table_id7}"

                client.delete_table(table_ref1, not_found_ok=True)
                client.delete_table(table_ref2, not_found_ok=True)
                client.delete_table(table_ref3, not_found_ok=True)
                client.delete_table(table_ref4, not_found_ok=True)
                client.delete_table(table_ref5, not_found_ok=True)
                client.delete_table(table_ref6, not_found_ok=True)
                client.delete_table(table_ref7, not_found_ok=True)
                print("Successfully all unused table")
            except Exception as e:
                print(f"Error deleting unused table: {str(e)}")


            # Halt trigger function2 scheduler job
            try:    
                client_sched.pause_job(name=job_name1)
                print("Successfully pause scheduler job4")
            except Exception as e:
                print(f"Error pausing scheduler job4: {str(e)}")

            return 'Completed successfully'
         
        else:
            pass

        std_time.sleep(300) 
        print("Data Quality and Peak Util not yet completed")

        t = std_time.time() - start_time




    
    return 'Completed successfully'
