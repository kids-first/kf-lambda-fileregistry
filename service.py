import os
import boto3
import json
import time
from botocore.vendored import requests
from base64 import b64decode


s3 = boto3.client("s3")


DATA_TYPES = {
    'fq': 'Unaligned Reads',
    'fastq': 'Unaligned Reads',
    'fq.gz': 'Unaligned Reads',
    'fastq.gz': 'Unaligned Reads',
    'bam': 'Aligned Reads',
    'cram': 'Aligned Reads',
    'bam.bai': 'Aligned Reads Index',
    'cram.crai': 'Aligned Reads Index',
    'g.vcf.gz': 'gVCF',
    'g.vcf.gz.tbi': 'gVCF Index'
}


FILE_FORMATS = {
    'fq': 'fq',
    'fastq': 'fq',
    'fq.gz': 'fq',
    'fastq.gz': 'fq',
    'bam': 'bam',
    'cram': 'cram',
    'bam.bai': 'bai',
    'cram.crai': 'crai',
    'g.vcf.gz': 'gVCF',
    'g.vcf.gz.tbi': 'tbi'
}


class ImportException(Exception):
        pass

class DataServiceException(Exception):
        pass

class CavaticaException(Exception):
        pass

def handler(event, context):
    """
    Register a genomic file in dataservice from a list of s3 events.
    If all events are not processed before the lambda runs out of time,
    the remaining will be submitted to a new function
    """
    DATASERVICE_API = os.environ.get('DATASERVICE_API', None)
    if DATASERVICE_API is None:
        return 'no dataservice url set'

    TOKEN = os.environ.get('CAVATICA_TOKEN', None)
    CAVATICA_TOKEN = None
    if TOKEN:
        CAVATICA_TOKEN = boto3.client('kms').decrypt(CiphertextBlob=b64decode(TOKEN)).get('Plaintext', None)
        HEADERS = {'X-SBG-Auth-Token': CAVATICA_TOKEN}

    importer = FileImporter(DATASERVICE_API, CAVATICA_TOKEN)
    res = {}
    for i, record in enumerate(event['Records']):

        # If we're running out of time, stop processing and re-invoke
        # NB: We check that i > 0 to ensure that *some* progress has been made
        # to avoid infinite call chains.
        if (hasattr(context, 'invoked_function_arn') and
            context.get_remaining_time_in_millis() < 500 and
            i > 0):
            records = event['Records'][i:]
            print('not able to complete {} records, '
                  're-invoking the function'.format(len(records)))
            remaining = {'Records': records}
            lam = boto3.client('lambda')
            context.invoked_function_arn
            # Invoke the lambda again with remaining records
            response = lam.invoke(
                FunctionName=context.invoked_function_arn,
                InvocationType='Event',
                Payload=str.encode(json.dumps(remaining))
            )
            # Stop processing and exit
            break

        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        name = '{}/{}'.format(bucket, key)
        res[name] = importer.import_from_event(record)
    else:
        print('processed all records')

    return res


class FileImporter:

    def __init__(self, api, cavatica_token):
        self.api = api
        self.cavatica_token = cavatica_token

    def import_from_event(self, event):
        """
        Processes a single record from an s3 event
        """
        res = {'harmonized': 'not imported', 'source': 'not imported'}
        try:
            tags = self.import_harmonized(event)
            res['harmonized'] = 'imported'
        except (DataServiceException, ImportException) as err:
            res['harmonized'] = str(err)
            return res

            
        try:
            self.register_input(tags)
            res['source'] = 'imported'
        except (DataServiceException, ImportException) as err:
            res['source'] = str(err)


        return res

    def import_harmonized(self, record):
        """
        Imports a harmonized file from an s3 event record

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
        it is a pre-determined kf_id and use it when creating a new genomic
        file.

        If the biospecimen that is referenced in the `bs_id` tag is not
        found in the dataservice, abort the import.
        
        Once the genomic file has been imported to the dataservice, tag the
        object with the kf_id under the `gf_id` tag, unless there was already a
        `gf_id` field there.
        """
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        tags = s3.get_object_tagging(Bucket=bucket, Key=key)
        tags = {t['Key']: t['Value'] for t in tags['TagSet']}

        # Update if no study_id
        if 'study_id' not in tags:
            study_id = '_'.join(bucket.split('-')[-2:]).upper()
            tags['study_id'] = study_id
            tagset = {'TagSet': [{'Key': k, 'Value': v} for k, v in tags.items()]}
            r = s3.put_object_tagging(Bucket=bucket, Key=key, Tagging=tagset)
        study_id = tags['study_id']

        # Skip if there is a kf_id assigned already and exists in dataservice
        gf_id = self.get_gf_id_tag(tags)

        req_tags = ['cavatica_harmonized_file', 'cavatica_source_file',
                    'cavatica_app', 'bs_id', 'cavatica_source_path',
                    'cavatica_task']

        # Make sure the required tags are there
        missing = [tag for tag in req_tags if tag not in tags]
        if len(missing) > 0:
            raise ImportException('missing required tag(s) {}'.format(missing))

        # Check that the biospecimen exists
        resp = requests.get(self.api+'biospecimens/'+tags['bs_id'])
        if resp.status_code != 200:
            raise ImportException('biospecimen matching bs_id does not exist')

        gf = self.new_file(bucket, key, record['s3']['object']['eTag'],
                           record['s3']['object']['size'], gf_id=gf_id,
                           bs_id=tags['bs_id'], study_id=study_id)

        # Update tags if no gf_id
        if gf_id is None:
            tags['gf_id'] = gf['kf_id']
            tagset = {'TagSet': [{'Key': k, 'Value': v} for k, v in tags.items()]}
            r = s3.put_object_tagging(Bucket=bucket, Key=key, Tagging=tagset)

        return tags


    def new_file(self, bucket, key, etag, size,
                 gf_id=None, bs_id=None, study_id=None):
        """
        Creates a new genomic file in the dataservice

        :param bucket: The bucket of the object
        :param key: The key of the object
        :param etag: The ETag of the object
        :param size: The size in bytes of the object
        :param gf_id: Optional kf_id for the genomic file
        """
        file_name = key.split('/')[-1]
        hashes = {'etag': etag.replace('"', '')}
        urls = ['s3://{}/{}'.format(bucket, key)]
        file_format = key.split('/')[-1].lower()
        file_format = file_format[file_format.find('.')+1:]
        data_type = DATA_TYPES[file_format]
        if file_format in FILE_FORMATS:
            file_format = FILE_FORMATS[file_format]
        harmonized = key.startswith('harmonized/')

        gf = {
            'file_name': file_name,
            'file_format': file_format,
            'data_type': data_type,
            'availability': 'Immediate Download',
            'controlled_access': True,
            'is_harmonized': harmonized,
            'hashes': hashes,
            'size': size,
            'urls': urls
        }

        if gf_id:
            gf['kf_id'] = gf_id
        if bs_id:
            gf['biospecimen_id'] = bs_id
        if study_id:
            gf['acl'] = [ study_id ]

        resp = requests.post(self.api+'genomic-files', json=gf)

        if (resp.status_code != 201 or
            'results' not in resp.json() or
            'kf_id' not in resp.json()['results']):
            raise DataServiceException('bad dataservice response')

        return resp.json()['results']

    def get_gf_id_tag(self, tags):
        """
        Returns a gf_id after verifying that it exists in list of tags
        and checking that it does not yet exist in the dataservice.

        If there is a `gf_id` tag in the tagset, try to look up that kf_id
        in the dataservice. If the dataservice does not return 404, assume
        the genomic file has already been imported and raise an exception.

        :param tags: The tags on the object as a {name: value} dict
        :returns: a kf_id of a genomic file, if the tagset contains a `gf_id`
            tag with a kf_id that does not exist in the dataservice,
            `None` otherwise
        :raises: `ImportException` if a file with the matching kf_id already
            exists in the dataservice
        """
        gf_id = None
        if 'gf_id' in tags:
            url = self.api+'genomic-files/'+tags['gf_id']
            resp = requests.get(url)
            if resp.status_code != 404 and 'results' in resp.json():
                raise ImportException(tags['gf_id'] + ' already registered')
            # Save for later so we can import with pre-determined id
            gf_id = tags['gf_id']
        return gf_id


    def register_input(self, harm_tags):
        """
        Registers a source genomic file given an s3 path
        """
        source_path = harm_tags['cavatica_source_path']
        bucket = source_path.replace('s3://', '').split('/')[0]
        key = '/'.join(source_path.split('/')[1:])

        tags = s3.get_object_tagging(Bucket=bucket, Key=key)
        tags = {t['Key']: t['Value'] for t in tags['TagSet']}

        study_id = harm_tags['study_id']

        gf_id = self.get_gf_id_tag(tags)

        obj = s3.get_object(Bucket=bucket, Key=key)
        
        gf = self.new_file(bucket, key, obj['ETag'], obj['ContentLength'],
                           bs_id=harm_tags['bs_id'], study_id=study_id)

        # Update tags if study_id or gf_id weren't in the tags
        if gf_id is None or 'study_id' not in tags:
            tags['gf_id'] = gf['kf_id']
            tags['study_id'] = harm_tags['study_id']
            tags['bs_id'] = harm_tags['bs_id']
            tagset = {'TagSet': [{'Key': k, 'Value': v} for k, v in tags.items()]}
            r = s3.put_object_tagging(Bucket=bucket, Key=key, Tagging=tagset)
