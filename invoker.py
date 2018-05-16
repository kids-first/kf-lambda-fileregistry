import copy
import os
import json
import boto3
from botocore.vendored import requests


record_template = {
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


BATCH_SIZE = 10
SLACK_TOKEN = os.environ.get('SLACK_SECRET', None)
SLACK_CHANNELS = os.environ.get('SLACK_CHANNEL', '').split(',')
SLACK_CHANNELS = [c.replace('#','').replace('@','') for c in SLACK_CHANNELS]


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

    
    attachments = [
        { "fallback": "I'm about to import all files under `{}/{}`, hold tight...".format(bucket, prefix),
          "text": "I'm about to import all files under `{}/{}`, hold tight...".format(bucket, prefix),
          "color": "#005e99"
        }
    ]
    send_slack(attachments=attachments)
        
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    lam = boto3.client('lambda')
    s3cl = boto3.client('s3')
    
    records = 0
    invoked = 0
    events = []

    for i, k in enumerate(bucket.objects.filter(Prefix=prefix)):
        # Send warning message if little time remaining
        if context.get_remaining_time_in_millis()/1000 < 1:
            attachments = [
                { "fallback": "Ran out of time for `{}/{}`".format(bucket.name, prefix),
                  "text": "Ran out of time for `{}/{}`".format(bucket.name, prefix),
                  "fields": [
                      {
                          "title": "Files Imported",
                          "value": records,
                          "short": True
                      },
                      {
                          "title": "Function Calls",
                          "value": invoked,
                          "short": True
                      }
                  ],
                  "color": "danger"
                }
            ]
            send_slack(attachments=attachments)
            break

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

    # Slack notif
    attachments = [
        { "fallback": "Finished invokes for `{}/{}` with *{}s* remaining".format(bucket.name, prefix, context.get_remaining_time_in_millis()/1000),
          "text": "Finished invokes for `{}/{}` with *{}s* remaining".format(bucket.name, prefix, context.get_remaining_time_in_millis()/1000),
          "fields": [
              {
                  "title": "Files Imported",
                  "value": records,
                  "short": True
              },
              {
                  "title": "Function Calls",
                  "value": invoked,
                  "short": True
              }
          ],
          "color": "good"
        }
    ]
    send_slack(attachments=attachments)

    return '{} records processed in {} calls'.format(records, invoked)


def send_slack(msg=None, attachments=None):
    """
    Sends a slack notification
    """
    if SLACK_TOKEN is not None:
        for channel in SLACK_CHANNELS:
            message = {
                'username': 'File Registry Bot',
                'icon_emoji': ':file_folder:',
                'channel': channel
            }
            if msg:
                message['text'] = msg
            if attachments:
                message['attachments'] = attachments

            resp = requests.post('https://slack.com/api/chat.postMessage',
                headers={'Authorization': 'Bearer '+SLACK_TOKEN},
                json=message)


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
    ev = copy.deepcopy(record_template)
    ev["s3"]["bucket"]["name"] = bucket
    ev["s3"]["bucket"]["arn"] = 'arn:aws:s3:::'+bucket
    ev["s3"]["object"]["key"] = key
    ev["s3"]["object"]["size"] = size
    ev["s3"]["object"]["eTag"] = e_tag.replace('"', '')
    
    return ev
