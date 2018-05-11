import os
import json
import pytest
import boto3
from moto import mock_s3
from mock import patch, MagicMock
import service

BUCKET = 'kf-study-us-east-1-dev-sd-9pyzahhe'
OBJECT = 'harmonized/cram/60d33dec-98db-446c-ac64-f4d027588f26.cram'

SOURCE_BUCKET = 'kf-seq-data-washu'
SOURCE_OBJECT = 'OrofacialCleft/bd042b24ae844a57ace28cf70cb3c852.bam'

TAGS = {
    'TagSet': [
        {
            'Key': 'cavatica_harmonized_file',
            'Value': '5aea288dec701d183bbbdda6'
        },
        {
            'Key': 'cavatica_source_file',
            'Value': '5ae2085bec701d183bbab7b3'
        },
        {
            'Key': 'cavatica_app',
            'Value': 'kfdrc-harmonization/sd-9pyzahhe-03/kfdrc-alignment-workflow/2'
        },
        {
            'Key': 'bs_id',
            'Value': 'BS_QV3Z0DZM'
        },
        {
            'Key': 'cavatica_source_path',
            'Value': 'kf-seq-data-washu/OrofacialCleft/bd042b24ae844a57ace28cf70cb3c852.bam'
        },
        {
            'Key': 'cavatica_task',
            'Value': '00025011-9dd7-40a6-8141-853323885e61'
        }
    ]
}


@pytest.fixture(scope='function')
def obj():
    @mock_s3
    def with_obj():
        """ Create a harmonized file and its source file """
        s3 = boto3.client('s3')
        b = s3.create_bucket(Bucket=BUCKET)
        ob = s3.put_object(Bucket=BUCKET, Key=OBJECT, Body=b'test')
        # Tag with required fields
        response = s3.put_object_tagging(
            Bucket=BUCKET, Key=OBJECT, Tagging=TAGS
        )

        source_b = s3.create_bucket(Bucket=SOURCE_BUCKET)
        source_ob = s3.put_object(Bucket=SOURCE_BUCKET, Key=SOURCE_OBJECT,
                                  Body=b'test')

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
    os.environ['DATASERVICE_API'] = 'http://api.com/'
    mock = patch('service.requests')
    req = mock.start()
    mock_resp = MagicMock()
    mock_resp.json.return_value = {'results': {'kf_id': 'GF_00000000'}}
    mock_resp.status_code = 201
    req.post.return_value = mock_resp

    s3 = boto3.client('s3')

    res = service.handler(event, {})

    assert len(res) == 1
    k = '{}/{}'.format(BUCKET, OBJECT)
    assert res[k]['source'] == 'imported'
    assert res[k]['harmonized'] == 'imported'

    # Should be called once for the harmonized file, once for the source file
    assert req.post.call_count == 2

    # Check harmonized file call
    expected = {
        'file_name': '60d33dec-98db-446c-ac64-f4d027588f26.cram',
        'file_format': 'cram',
        'data_type': 'Aligned Reads',
        'controlled_access': True,
        'availability': 'Immediate Download',
        'is_harmonized': True,
        'hashes': {'etag': 'd41d8cd98f00b204e9800998ecf8427e'},
        'size': 1024,
        'urls': ['s3://{}/{}'.format(BUCKET, OBJECT)]
    }
    req.post.assert_any_call('http://api.com/genomic-files', json=expected)

    # Check source file call
    expected = {
        'file_name': 'bd042b24ae844a57ace28cf70cb3c852.bam',
        'file_format': 'bam',
        'data_type': 'Aligned Reads',
        'controlled_access': True,
        'availability': 'Immediate Download',
        'is_harmonized': False,
        'hashes': {'etag': '098f6bcd4621d373cade4e832627b4f6'},
        'size': 4,
        'urls': ['s3://{}/{}'.format(SOURCE_BUCKET, SOURCE_OBJECT)]
    }
    req.post.assert_any_call('http://api.com/genomic-files', json=expected)

    # Check that the harmonized file has been updated with the new kf_id
    response = s3.get_object_tagging(Bucket=BUCKET, Key=OBJECT)
    tags = {t['Key']: t['Value'] for t in response['TagSet']}
    assert 'gf_id' in tags

    # Check that the source file has been updated with the new kf_id
    response = s3.get_object_tagging(Bucket=SOURCE_BUCKET, Key=SOURCE_OBJECT)
    tags = {t['Key']: t['Value'] for t in response['TagSet']}
    assert 'gf_id' in tags

    mock.stop()


@mock_s3
def test_existing_gf(event, obj):
    """ Test that a new genomic file is not created if it exists """
    obj()
    s3 = boto3.client('s3')
    # Add a gf_id
    tags = TAGS.copy()
    tags['TagSet'].append({'Key': 'gf_id', 'Value': 'GF_00000001'})
    response = s3.put_object_tagging(
        Bucket=BUCKET, Key=OBJECT, Tagging=tags
    )

    os.environ['DATASERVICE_API'] = 'http://api.com/'
    mock = patch('service.requests')
    req = mock.start()
    mock_resp = MagicMock()
    mock_resp.json.return_value = {'results': {'kf_id': 'GF_00000001'}}
    mock_resp.status_code = 200
    req.get.return_value = mock_resp

    service.handler(event, {})

    assert req.get.call_count == 1
    assert req.post.call_count == 0

    mock.stop()


@mock_s3
def test_existing_gf_id(event, obj):
    """ Test that a new genomic file is created with predefined kf_id  """
    obj()
    s3 = boto3.client('s3')
    # Add a gf_id
    tags = TAGS.copy()
    tags['TagSet'].append({'Key': 'gf_id', 'Value': 'GF_00000002'})
    response = s3.put_object_tagging(
        Bucket=BUCKET, Key=OBJECT, Tagging=tags
    )

    os.environ['DATASERVICE_API'] = 'http://api.com/'
    mock = patch('service.requests')
    req = mock.start()
    mock_resp = MagicMock()
    mock_resp.status_code = 404
    req.get.return_value = mock_resp

    service.handler(event, {})

    expected = {
        'kf_id': 'GF_00000002',
        'file_name': '60d33dec-98db-446c-ac64-f4d027588f26.cram',
        'file_format': 'cram',
        'data_type': 'Aligned Reads',
        'controlled_access': True,
        'availability': 'Immediate Download',
        'is_harmonized': True,
        'hashes': {'etag': 'd41d8cd98f00b204e9800998ecf8427e'},
        'size': 1024,
        'urls': ['s3://{}/{}'.format(BUCKET, OBJECT)]
    }

    assert req.get.call_count == 1
    req.post.assert_called_with('http://api.com/genomic-files', json=expected)
    assert req.post.call_count == 1

    mock.stop()


def test_new_file():
    """ Test that new file are created correctly """
    os.environ['DATASERVICE_API'] = 'http://api.com/'
    mock = patch('service.requests')
    req = mock.start()
    mock_resp = MagicMock()
    mock_resp.status_code = 201
    mock_resp.json.return_value = {'results': {'kf_id': 'GF_00000000'}}
    req.post.return_value = mock_resp

    importer = service.FileImporter('http://api.com/', 'abc123')
    
    res = importer.new_file(BUCKET, OBJECT,
                            'd41d8cd98f00b204e9800998ecf8427e', 1024)

    expected = {
        'file_name': '60d33dec-98db-446c-ac64-f4d027588f26.cram',
        'file_format': 'cram',
        'data_type': 'Aligned Reads',
        'controlled_access': True,
        'availability': 'Immediate Download',
        'is_harmonized': True,
        'hashes': {'etag': 'd41d8cd98f00b204e9800998ecf8427e'},
        'size': 1024,
        'urls': ['s3://{}/{}'.format(BUCKET, OBJECT)]
    }

    req.post.assert_called_with('http://api.com/genomic-files', json=expected)
    assert req.post.call_count == 1


def test_new_file_gf_id():
    """ Test that new file with predefined kf_id """
    os.environ['DATASERVICE_API'] = 'http://api.com/'
    mock = patch('service.requests')
    req = mock.start()
    mock_resp = MagicMock()
    mock_resp.status_code = 201
    mock_resp.json.return_value = {'results': {'kf_id': 'GF_00000001'}}
    req.post.return_value = mock_resp

    importer = service.FileImporter('http://api.com/', 'abc123')
    
    res = importer.new_file(BUCKET, OBJECT,
                            'd41d8cd98f00b204e9800998ecf8427e', 1024,
                            gf_id='GF_00000001')

    expected = {
        'kf_id': 'GF_00000001',
        'file_name': '60d33dec-98db-446c-ac64-f4d027588f26.cram',
        'file_format': 'cram',
        'data_type': 'Aligned Reads',
        'controlled_access': True,
        'availability': 'Immediate Download',
        'is_harmonized': True,
        'hashes': {'etag': 'd41d8cd98f00b204e9800998ecf8427e'},
        'size': 1024,
        'urls': ['s3://{}/{}'.format(BUCKET, OBJECT)]
    }

    req.post.assert_called_with('http://api.com/genomic-files', json=expected)
    assert req.post.call_count == 1


def test_new_file_error():
    """ Test that new file with predefined kf_id """
    os.environ['DATASERVICE_API'] = 'http://api.com/'
    mock = patch('service.requests')
    req = mock.start()
    mock_resp = MagicMock()
    mock_resp.status_code = 400
    mock_resp.json.return_value = {'results': {'kf_id': 'GF_00000002'}}
    req.post.return_value = mock_resp

    importer = service.FileImporter('http://api.com/', 'abc123')
    
    with pytest.raises(service.DataServiceException):
        res = importer.new_file(BUCKET, OBJECT,
                                'd41d8cd98f00b204e9800998ecf8427e', 1024)

    assert req.post.call_count == 1
