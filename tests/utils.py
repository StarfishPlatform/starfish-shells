from faker import Faker


def identity(xs):
    return xs


def _gen_profile(fake):
    return {
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
