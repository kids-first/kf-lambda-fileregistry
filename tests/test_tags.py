import pytest
from mock import patch, MagicMock
import service


def test_get_gf_id_tag():
    """ Test that gf_id is returned from tags correctly """
    mock = patch('service.requests')
    req = mock.start()
    mock_resp = MagicMock()
    mock_resp.status_code = 404
    mock_resp.json.return_value = {'_status': {'code': 404}}
    req.post.return_value = mock_resp

    importer = service.FileImporter('http://api.com/', 'abc123')

    tags = {'gf_id': 'GF_00000003'}
    
    res = importer.get_gf_id_tag(tags)

    assert res == 'GF_00000003'
    assert req.get.call_count == 1

    mock.stop()


def test_get_gf_id_tag_exists():
    """ Test getting gf_id tag if it already exists """
    mock = patch('service.requests')
    req = mock.start()
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {'results': {'kf_id': 'GF_00000003'}}
    req.get.return_value = mock_resp

    importer = service.FileImporter('http://api.com/', 'abc123')

    tags = {'gf_id': 'GF_00000003'}
    
    with pytest.raises(service.ImportException):
        res = importer.get_gf_id_tag(tags)

    assert req.get.call_count == 1

    mock.stop()


def test_get_gf_id_tag_no_tag():
    """ Test getting gf_id tag if its not in the tags """
    importer = service.FileImporter('http://api.com/', 'abc123')
    tags = {'fake': 'tag'}
    res = importer.get_gf_id_tag(tags)
    assert res == None
