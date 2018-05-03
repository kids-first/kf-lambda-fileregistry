import os
import boto3
from botocore.vendored import requests


s3 = boto3.client("s3")

DATA_TYPES = {
    'fa': 'Unaligned Reads',
    'fq': 'Unaligned Reads',
    'fasta': 'Unaligned Reads',
    'fastq': 'Unaligned Reads',
    'bam': 'Submitted Aligned Reads',
    'cram': 'Submitted Aligned Reads',
    'bai': 'Submitted Aligned Reads Index',
    'crai': 'Submitted Aligned Reads Index',
    'vcf': 'Population Variant Calls',
    'gvcf': 'Individual Variant Calls'
}


def handler(event, context):
    """
    Register a genomic file in dataservice
    """
    DATASERVICE_API = os.environ.get('DATASERVICE_API', None)
    if DATASERVICE_API is None:
        return 'no dataservice url set'

    res = {}
    for record in event['Records']:
        r = process_record(DATASERVICE_API, record)
        res[record['s3']['object']['key']] = r

    return res

def process_record(api, record):
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']

    tags = s3.get_object_tagging(Bucket=bucket, Key=key)
    tags = {t['Key']: t['Value'] for t in tags['TagSet']}

    # Skip if there is a kf_id assigned already and exists in dataservice
    gf_id = None
    if 'gf_id' in tags:
        url = api+'genomic-files/'+tags['gf_id']
        resp = requests.get(url)
        if resp.status_code == 200:
            return 'already imported'
        # Save for later so we can import with pre-determined id
        gf_id = tags['gf_id']

    req_tags = ['cavatica_harmonized_file', 'cavatica_source_file',
                'cavatica_app', 'bs_id', 'cavatica_source_path',
                'cavatica_task']

    # Make sure the required tags are there
    for tag in req_tags:
        if tag not in tags:
            return 'missing required tag {}'.format(tag)

    file_name = key.split('/')[-1]
    hashes = {'etag': record['s3']['object']['eTag']}
    size = record['s3']['object']['size']
    urls = ['s3://{}/{}'.format(bucket, key)]
    file_format = key.split('/')[-1].split('.')[1].lower()
    data_type = DATA_TYPES[file_format.lower()]

    gf = {
        'file_name': file_name,
        'file_format': file_format,
        'data_type': data_type,
        'availability': 'Immediate Download',
        'controlled_access': True,
        'is_harmonized': True,
        'hashes': hashes,
        'size': size,
        'urls': urls
    }
    # There is already a kf_id tagged on the file, so use that
    if gf_id is not None:
        gf['kf_id'] = gf_id

    resp = requests.post(api+'genomic-files', json=gf)

    if ('results' not in resp.json() and
        'kf_id' not in resp.json()['results']):
        return 'bad response from dataservice'

    # Update tags with gf_id
    tags['gf_id'] = resp.json()['results']['kf_id']
    tagset = {'TagSet': [{'Key': k, 'Value': v} for k, v in tags.items()]}
    r = s3.put_object_tagging(Bucket=bucket, Key=key, Tagging=tagset)
    return 'imported'


def register_input(source_path):
        bucket = source_path.split('_')[0]
        key = '/'.join(source_path.split('/')[1:])
        
        file_name = key.split('/')[-1]
        hashes = {'etag': record['s3']['object']['eTag']}
        size = record['s3']['object']['size']
        urls = ['s3://{}/{}'.format(bucket, key)]
        file_format = key.split('/')[-1].split('.')[1].lower()
        data_type = DATA_TYPES[file_format.lower()]

        gf = {
            'file_name': file_name,
            'file_format': file_format,
            'data_type': data_type,
            'availability': 'Immediate Download',
            'controlled_access': True,
            'hashes': hashes,
            'size': size,
            'urls': urls
        }
