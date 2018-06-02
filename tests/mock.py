import socket
from functools import wraps
from threading import Thread

import pytest
import requests
from flask import jsonify, request

from starfish_shell import DIR_INPUT, DIR_OUTPUT


def get_free_port():
    s = socket.socket(socket.AF_INET, type=socket.SOCK_STREAM)
    s.bind(('localhost', 0))
    address, port = s.getsockname()
    s.close()
    return port


class MockServer(Thread):
    def __init__(self, port=None):
        super().__init__()
        from flask import Flask
        self.port = port or get_free_port()
        self.app = Flask(__name__)
        self.url = "http://localhost:%s" % self.port

        self.app.add_url_rule("/shutdown", view_func=self._shutdown_server)

    def _shutdown_server(self):
        from flask import request
        if not 'werkzeug.server.shutdown' in request.environ:
            raise RuntimeError('Not running the development server')
        request.environ['werkzeug.server.shutdown']()
        return 'Server shutting down...'

    def shutdown_server(self):
        requests.get("http://localhost:%s/shutdown" % self.port)
        self.join()

    def add_callback_response(self, url, callback, methods=('GET',)):
        self.app.add_url_rule(url, view_func=callback, methods=methods)

    def add_json_response(self, url, callback, methods=('GET',)):
        @wraps(callback)
        def _callback(*args, **kwargs):
            return jsonify(callback(*args, **kwargs))

        self.add_callback_response(url, _callback, methods=methods)

    def run(self):
        self.app.run(port=self.port)


@pytest.fixture
def mock_server():
    mock_server = None
    try:
        mock_server = MockServer()
        mock_server.start()
        yield mock_server
    finally:
        if mock_server is not None:
            mock_server.shutdown_server()


class StarfishLogs:
    def __init__(self, content):
        self._content = content

    def __len__(self):
        return len(self._content)

    @property
    def sources(self):
        return {x.get('storage', None)
                for x in self._content
                if x.get('direction', None) == DIR_INPUT}

    @property
    def destinations(self):
        return {x.get('storage', None)
                for x in self._content
                if x.get('direction', None) == DIR_OUTPUT}


class MockStarfishAPI(MockServer):
    def __init__(self, port=None):
        super().__init__(port=port)
        self.add_json_response('/logs/<service_id>/<run_id>', self._add_log, methods=('POST',))
        self.add_json_response(
            '/', lambda: {'status': 'ready'}
        )
        self._logs = []

    @property
    def logs(self):
        return StarfishLogs(self._logs)

    def _add_log(self, service_id, run_id):
        json = request.get_json()
        self._logs.append({'service_id': service_id, 'run_id': run_id, **json})


@pytest.fixture
def mock_starfish():
    mock_server = None
    try:
        mock_server = MockStarfishAPI()
        mock_server.start()
        yield mock_server
    finally:
        if mock_server is not None:
            mock_server.shutdown_server()
