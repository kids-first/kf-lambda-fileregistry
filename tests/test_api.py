import os
import boto3
import json
import pytest
import service
from mock import patch
from moto import mock_s3


BUCKET = 'kf-study-us-east-1-dev-sd-00000000'
OBJECT = 'harmonized/reads/hg38.bam'


@pytest.fixture(scope='session')
def obj():
    @mock_s3
    def with_obj():
        s3 = boto3.client('s3')
        b = s3.create_bucket(Bucket=BUCKET)
        ob = s3.put_object(Bucket=BUCKET, Key=OBJECT, Body=b'test')
        return ob
    return with_obj

@pytest.fixture(scope='session')
def event():
    """ Returns a test s3 event """
    cur = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(cur, 's3_event.json')) as f:
        data = json.load(f)
    return data

@mock_s3
def test_create(event, obj):
    """ Test that the lamba calls the dataservice """
    obj()
    os.environ['DATASERVICE_API'] = 'http://blah.com'
    mock = patch('service.requests')
    req = mock.start()

    s3 = boto3.client('s3')

    service.handler(event, {})

    expected = {
        'file_name': 'hg38.bam',
        'file_format': 'bam',
        'data_type': 'Aligned Reads',
        'availability': 'available',
        'controlled_access': True,
        'harmonized': True,
        'hashes': {'etag': 'd41d8cd98f00b204e9800998ecf8427e'},
        'size': 1024,
        'urls': ['s3://kf-study-us-east-1-dev-sd-00000000/harmonized/reads/hg38.bam']
    }

    assert req.post.call_count == 1
    req.post.assert_called_with('http://blah.com/genomic-files', json=expected)

    mock.stop()
