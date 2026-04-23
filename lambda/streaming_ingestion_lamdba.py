import json
import boto3
import urllib.request

firehose = boto3.client('firehose')
FIREHOSE_STREAM_NAME = "OpenAlex-stream-ingestion"
def lambda_handler(event, context):
    url="https://api.openalex.org/works?per-page=5"
    response = urllib.request.urlopen(url)
    data = json.loads(response.read())
    records = []
    for item in data["results"]:
        records.append({
            "Data":json.dumps(item)+"\n"
        })

    firehose.put_record_batch(
        DeliveryStreamName=FIREHOSE_STREAM_NAME,
        Records=records)
    return {
        'statusCode': 200,
        'body': "Records sent to firehose",
        'records_sent':len(records)
    }