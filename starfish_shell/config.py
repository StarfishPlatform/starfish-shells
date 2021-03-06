import hashlib
import os

import time


def default_matcher(x):
    return x['userID']


def hash_id(matched):
    m = hashlib.sha256()
    m.update(matched.encode('utf-8'))
    return m.hexdigest()


PROFILE_MATCHERS = {
    'default': default_matcher,
}


def get_profile_matcher(matcher):
    if callable(matcher):
        return matcher
    else:
        return PROFILE_MATCHERS[matcher]


class Config:
    def __init__(self, api_url, service_id, run_id, profile_matcher='default', batch_size=20):
        self._api_url = api_url
        self._service_id = service_id
        self._run_id = run_id
        self._profile_matcher = get_profile_matcher(profile_matcher)
        self.batch_size = batch_size

    def __eq__(self, other):
        if not isinstance(other, Config):
            return False

        return (self._api_url == other._api_url
                and self._service_id == other._service_id
                and self._run_id == other._run_id)

    @classmethod
    def from_env(cls, matcher=None):
        env = os.environ
        api_url = env['STARFISH_API_URL']
        service_id = env['STARFISH_SERVICE_ID']
        run_id = env.get('STARFISH_RUN_ID', None)

        if run_id is None:
            # TODO: Add info logging when this case arise
            run_id = str(int(time.time() * 1000))

        c = Config(api_url, service_id, run_id)
        if matcher:
            c = c.with_matcher(matcher)
        return c

    def build_query_for(self, profiles, direction, storage):
        ts = str(int(time.time() * 1000))
        # TODO: reflect on whether this is good SoC (hint: probably not) and refactor
        payload = dict(
            userIDs=[hash_id(self._profile_matcher(profile)) for profile in profiles],
            timestamp=ts,
            direction=direction,
            storage=storage
        )

        return dict(
            url=f'{self._api_url}/logs/{self._service_id}/{self._run_id}',
            json=payload
        )

    def with_matcher(self, matcher):
        return self.__class__(self._api_url, self._service_id, self._run_id, matcher)

    @property
    def is_online(self):
        return True


class ConfigTestOffline(Config):
    def __init__(self, api_url=None, service_id=None, run_id=None, profile_matcher='default', batch_size=1):
        self.queries = []
        self.logs = []
        super().__init__(api_url or None,
                         service_id or 'some-service',
                         run_id or 'some-run',
                         profile_matcher=profile_matcher or 'default',
                         batch_size=batch_size)

    def build_query_for(self, profiles, direction, storage):
        query = super().build_query_for(profiles, direction, storage)
        self.queries.append(query)
        self.logs.append(query['json'])
        return query

    @property
    def is_online(self):
        return False
