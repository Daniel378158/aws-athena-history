import json
import boto3
import csv
import os
import datetime


def create_workgroup_history(day: str, workgroup: str) -> str:
    file_name = f"/tmp/{workgroup}_queries.csv" 
    
    with open(file_name, "w", newline='') as csv_file:
        athena = boto3.client("athena")
        paginator = athena.get_paginator("list_query_executions").paginate(WorkGroup=workgroup)
        writer = csv.writer(csv_file)
        for page in paginator:
            query_executions = athena.batch_get_query_execution(QueryExecutionIds=page["QueryExecutionIds"])
            for query in query_executions["QueryExecutions"]:
                if "CompletionDateTime" not in query["Status"]:
                    continue
                query_day = query["Status"]["CompletionDateTime"].strftime("%Y-%m-%d")
                if day == query_day:
                    writer.writerow([query["QueryExecutionId"], query["Statistics"]["DataScannedInBytes"]])
                elif query_day < day:
                    return file_name

def upload_file_to_s3(_file_path, _bucket, _key):
    s3_client = boto3.client('s3')
    s3_client.upload_file(_file_path, _bucket, _key)

def lambda_handler(event, context):
    # TODO implement
    
    today = datetime.datetime.now()
    target_day = (today - datetime.timedelta(days=4)).strftime("%Y-%m-%d")
    
    target_workgroup = "primary"
    # target_workgroup = "Daniel-wrokgroup"


    result_file = create_workgroup_history(target_day, target_workgroup)

    s3_bucket = "daniel-s3-test-bucket"
    s3_key_prefix = "history/"  

    
    s3_key = f"{s3_key_prefix}year={target_day[:4]}/month={target_day[5:7]}/day={target_day[8:10]}/{os.path.basename(result_file)}"

    upload_file_to_s3(result_file, s3_bucket, s3_key)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
