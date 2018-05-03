Kids First File Registry Lambda 
===============================

A lambda to listen for new files and import them to the Data Service


# Testing

The function may be tested using sam local.
This will create an event and invoke it against the function. The function will
make calls to live aws s3 and dataservice, so ensure that those services
are accessible.
```
sam local generate-event s3 --bucket kf-study-us-east-1-dev-sd-9pyzahhe --key harmonized/cram/BS_H14CBVAQ.cram  | DATASERVICE_API=http://kf-api-dataservice-dev.kids-first.io sam local invoke fileregistry -t template.yml
```
