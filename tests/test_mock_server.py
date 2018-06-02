import requests


def test_mock_server_simple(mock_server):
    mock_server.add_json_response('/', lambda: {'helloworld': True})

    r = requests.get(mock_server.url)

    assert r.status_code == 200
    assert r.json() == {'helloworld': True}


def test_mock_server_params(mock_server):
    mock_server.add_json_response('/<name>', lambda name: {'hello': name})

    r = requests.get(f'{mock_server.url}/john-doe')

    assert r.status_code == 200
    assert r.json() == {'hello': 'john-doe'}


def test_mock_starfish(mock_starfish):
    r = requests.get(f'{mock_starfish.url}/')
    assert r.status_code == 200
