Kids First File Registry Lambda 
===============================

A lambda to listen for new files and import them to the Data Service

# Operation

The function will attempt to import a file from s3 using tags on the object.

The event recieved by the lambda is expected to be for the harmonized file.

A genomic file for the source file will attempt to be imported using the
`cavatica_source_path` tag.

Both the source file and the harmonized file will attempt to be linked to the
biospecimen by looking up the `bs_id` in the dataservice. If no biospecimen
is found, no genomic files will be created.

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
