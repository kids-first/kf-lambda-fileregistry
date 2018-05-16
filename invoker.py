import os
import json
import boto3
import botocore


record_template = {
  {
     "s3":{
        "bucket":{
           "name":"kf-study-us-east-1-dev-sd-9pyzahhe",
           "arn":"kf-study-us-east-1-dev-sd-9pyzahhe"
        },
        "object":{
           "key": None,
           "size": None,
           "eTag": None
        }
     }
  }
}

BATCH_SIZE = 10


def handler(event, context):
    """
    Scans a bucket+prefix and invokes the fileregistry lambda for every
    object found in batches of 10 records.

    Will recieve an event of the form:
    ```
    {
        "bucket": "kf-study-us-east-1-dev-sd-0000000",
        "prefix": "harmonized/"
    }
    ```
    Where the prefix is optional and will default to the entire bucket.
    """
    bucket = event.get('bucket', None)
    # The fileregistry lambda ARN
    fileregistry = os.environ.get('FILEREGISTRY', None)
    if bucket is None or fileregistry is None:
        return 'no bucket or lambda specified'

    prefix = event.get('prefix', '')
        
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    lam = boto3.client('lambda')
    s3cl = boto3.client('s3')
    
    records = 0
    invoked = 0
    events = []

    for i, k in enumerate(bucket.objects.filter(Prefix=prefix)):
        records += 1
        events.append(event_generator(bucket.name, k.key, k.size, k.e_tag))

        # Flush events
        if len(events) >= BATCH_SIZE:
            invoked += 1
            invoke(lam, fileregistry, events)
            events = []

    if len(events) > 0:
        invoked += 1
        invoke(lam, fileregistry, events)

    return '{} records processed in {} calls'.format(records, invoked)


def invoke(lam, fileregistry, records):
    """
    Invokes the lambda for given records
    """
    payload = {'Records': records}
    response = lam.invoke(
        FunctionName=fileregistry,
        InvocationType='Event',
        Payload=str.encode(json.dumps(payload)),
    )


def event_generator(bucket, key, size, e_tag):
    ev = record_template.copy()
    ev["Records"][0]["s3"]["bucket"]["name"] = bucket
    ev["Records"][0]["s3"]["bucket"]["arn"] = 'arn:aws:s3:::'+bucket
    ev["Records"][0]["s3"]["object"]["key"] = key
    ev["Records"][0]["s3"]["object"]["size"] = size
    ev["Records"][0]["s3"]["object"]["eTag"] = e_tag
    
    return ev
