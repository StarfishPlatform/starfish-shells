import contextlib
import os
import random

from faker import Faker

REQUIRED_FIELDS = {'timestamp', 'userID', 'storage', 'direction', 'serviceID', 'runID'}


def random_matcher(profile):
    return str(random.randint(0, 10000))


def random_failure_matcher(profile):
    if random.random() > 0.5:
        raise Exception("My matcher failed!")
    return random_matcher(profile)


def identity(xs):
    return xs


def _gen_profile(fake):
    return {
        'userID': fake.name(),
        'name': fake.name(),
        'address': fake.address()
    }


def gen_profile():
    fake = Faker()
    fake.seed(1)
    return _gen_profile(fake)


def gen_profiles(count):
    fake = Faker()
    fake.seed(count)

    for i in range(count):
        yield _gen_profile(fake)


def noop_processing(profiles):
    for profile in profiles:
        yield profile


@contextlib.contextmanager
def env(**kwargs):
    old_values = {}

    try:
        for k, v in kwargs.items():
            # save
            if k in os.environ:
                old_values[k] = os.environ[k]

            # set
            os.environ[k] = v

        yield
    finally:
        for k, v in old_values.items():
            os.environ[k] = v


def check_log(log):
    missing_fields = REQUIRED_FIELDS - set(log.keys())
    assert not missing_fields, f'Missing fields: {missing_fields}'

    for field in REQUIRED_FIELDS:
        assert log[field], f'Empty field: {log[field]}'
    return True
