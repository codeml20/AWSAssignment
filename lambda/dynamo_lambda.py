import boto3
import datetime
import uuid

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('metadata_table')

def lambda_handler(event, context):
   
    glue_run_id = event.get("glue_job_run_id") or str(uuid.uuid4())
    status = event.get("status", "SUCCESS")
    table.put_item(
        Item={
            "run_id": glue_run_id,
            "timestamp": str(datetime.datetime.utcnow()),
            "status": status
        }
    )

    return {
        "status":200,
        "message": "Metadata stored",
        "glue_job_run_id": glue_run_id
    }