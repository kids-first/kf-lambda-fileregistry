import os
import boto3
import requests


s3 = boto3.client("s3")

DATA_TYPES = {
    'fa': 'Unaligned Reads',
    'fq': 'Unaligned Reads',
    'fasta': 'Unaligned Reads',
    'fastq': 'Unaligned Reads',
    'bam': 'Aligned Reads',
    'cram': 'Aligned Reads',
    'bai': 'Read Index',
    'crai': 'Read Index',
    'vcf': 'Population Variant Calls',
    'gvcf': 'Individual Variant Calls'
}


def handler(event, context):
    """
    Register a genomic file in dataservice
    """
    DATASERVICE_API = os.environ.get('DATASERVICE_API', None)
    if DATASERVICE_API is None:
        return

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key'] 
        version = record['s3']['object']['versionId'] 
        
        tags = s3.get_object_tagging(Bucket=bucket, Key=key)
        tags = {t['Key']: t['Value'] for t in tags['TagSet']}

        file_name = key.split('/')[-1]
        harmonized = key.split('/')[0] == 'harmonized'
        hashes = {'etag': record['s3']['object']['eTag']}
        size = record['s3']['object']['size']
        urls = ['s3://{}/{}'.format(bucket, key)]
        file_format = key.split('/')[-1].split('.')[1].lower()
        data_type = DATA_TYPES[file_format.lower()]


        gf = {
            'file_name': file_name,
            'file_format': file_format,
            'data_type': data_type,
            'availability': 'available',
            'controlled_access': True,
            'harmonized': harmonized,
            'hashes': hashes,
            'size': size,
            'urls': urls
        }

        resp = requests.post(DATASERVICE_API+'/genomic-files', json=gf)

    return 'ok'
