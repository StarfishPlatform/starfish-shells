class Config:
    def __init__(self, api_url, service_id, run_id):
        self._api_url = api_url
        self._service_id = service_id
        self._run_id = run_id

    def build_query_for(self, profile, direction, storage):
        payload = dict(
            direction=direction,
            storage=storage
        )

        return dict(
            url=f'{self._api_url}/logs/{self._service_id}/{self._run_id}',
            json=payload
        )

    @property
    def is_online(self):
        return True


class ConfigTestOffline(Config):
    def __init__(self):
        super().__init__(None, 'some-service', 'some-run')

    @property
    def is_online(self):
        return False
