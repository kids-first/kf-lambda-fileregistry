Kids First File Registry Lambda 
===============================

A lambda to listen for new files and import them to the Data Service

# Operation

The `invoker` will be called given a bucket and a prefix. All objects under that bucket and prefix will be the target of the main `service.handler()` in batches of up to 10 objects per invocation.

When the `service.handler()` is called given a list of up to 10 s3 events (see below), it will attempt to import, or update, a GenomicFile for that object.

For each object passed into the handler:

1) inspect the tags on that object
2) if there is no `study_id` tag, add it to the tags using the bucket name
3) if there is a `gf_id` tag, look up that `kf_id` in the dataservice to see if exists
  3a) if it exists in the dataservice, it's already been imported. Skip to 7) 
4) Check that the required tags are present, else stop importing: `['cavatica_harmonized_file', 'cavatica_source_file', 'cavatica_app', 'bs_id', 'cavatica_source_path', 'cavatica_task']`
5) Check that a Biospecimen exists in the dataservice matching the `bs_id` tag, else stop importing
6) Register a GenomicFile in the Dataservice
7) Repeat from 1) for the object at the `cavatica_source_path`

# Invocation

An example invocaction call for the lambda is shown below.
All information must be provided as is below. Multiple records may be
submitted at once.

```
import boto3
import json

event = {
  'Records': [
      {  
         "s3":{  
            "bucket":{  
               "name":"kf-study-us-east-1-dev-sd-9pyzahhe",
               "arn":"arn:aws:s3:::kf-study-us-east-1-dev-sd-9pyzahhe"
            },
            "object":{  
               "key":"harmonized/cram/60d33dec-98db-446c-ac64-f4d027588f26.cram",
               "size":1024,
               "eTag":"d41d8cd98f00b204e9800998ecf8427e"
            }
         }
      }
      # More records if desired...
   ]
}
lam = boto3.client('lambda')
response = lam.invoke(
  FunctionName='kf-lambda-file-registry-dev',     # Function name or arn
  InvocationType='Event',
  Payload=str.encode(json.dumps(event))           # Must be in bytes
)
```
