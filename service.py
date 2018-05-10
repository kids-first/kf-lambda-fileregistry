import os
import boto3
from botocore.vendored import requests


s3 = boto3.client("s3")

DATA_TYPES = {
    'fa': 'Unaligned Reads',
    'fq': 'Unaligned Reads',
    'fasta': 'Unaligned Reads',
    'fastq': 'Unaligned Reads',
    'bam': 'Aligned Reads',
    'cram': 'Aligned Reads',
    'bai': 'Aligned Reads Index',
    'crai': 'Aligned Reads Index',
    'g.vcf.gz': 'Variant Calls',
    'g.vcf.gz.tbi': 'Individual Variant Calls'
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
    """
    Imports a single file from an s3 event into the dataservice

    The object in the event must already be tagged with the following 
    required fields in order to be imported:

    - `cavatica_harmonized_file`
    - `cavatica_source_file`
    - `cavatica_app`
    - `bs_id`
    - `cavatica_source_path`
    - `cavatica_task`

    If there is a `gf_id` tag on the object already, check to see if that
    file already exists in the dataservice, if it does not, assume that
    it is a pre-determined kf_id and use it when creating a new genomic file.
    
    Once the genomic file has been imported to the dataservice, tag the object
    with the kf_id under the `gf_id` tag, unless there was already a `gf_id`
    field there.
    """
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
    file_format = key.split('/')[-1].lower()
    file_format = file_format[file_format.find('.')+1:]
    data_type = DATA_TYPES[file_format]

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

    # Update tags with gf_id if it wasn't already in the tags
    if gf_id is None:
        tags['gf_id'] = resp.json()['results']['kf_id']
        tagset = {'TagSet': [{'Key': k, 'Value': v} for k, v in tags.items()]}
        r = s3.put_object_tagging(Bucket=bucket, Key=key, Tagging=tagset)

    return 'imported'


def register_input(source_path):
    """
    Registers a source genomic file given an s3 path
    """
    bucket = source_path.split('_')[0]
    key = '/'.join(source_path.split('/')[1:])
    
    file_name = key.split('/')[-1]
    hashes = {'etag': record['s3']['object']['eTag']}
    size = record['s3']['object']['size']
    urls = ['s3://{}/{}'.format(bucket, key)]
    file_format = key.split('/')[-1].lower()
    file_format = file_format[file_format.find('.'):]
    data_type = DATA_TYPES[file_format]

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
