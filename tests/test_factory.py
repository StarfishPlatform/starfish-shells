from starfish_shell import ShellFactory, Config
from starfish_shell.config import ConfigTestOffline
from tests.utils import gen_profiles, identity, env

CONF_OFFLINE = ConfigTestOffline()


def test_i_can_create_the_factory():
    factory = ShellFactory(CONF_OFFLINE)
    assert factory is not None


def test_i_can_wrap_a_function_with_the_factory():
    factory = ShellFactory(CONF_OFFLINE)
    shelled = factory.shell_process(identity, source='some-source', destination='some-dest')
    assert shelled is not None


def test_build_factory_config_from_env():
    with env(STARFISH_API_URL='a',
             STARFISH_SERVICE_ID='b',
             STARFISH_RUN_ID='c'):
        config = ShellFactory.from_env().config
        assert config == Config(api_url='a', service_id='b', run_id='c')


def test_the_function_i_wrap_doesnt_change_input_outputs():
    factory = ShellFactory(CONF_OFFLINE.with_matcher('test_random'))
    shelled = factory.shell_process(identity, source='some-source', destination='some-dest')
    assert list(shelled([1, 2, 3])) == [1, 2, 3]


def test_the_result_has_starfish_details():
    factory = ShellFactory(CONF_OFFLINE)
    shelled = factory.shell_process(identity, source='some-source', destination='some-dest')
    assert shelled.starfish


def test_i_can_wrap_iterators():
    factory = ShellFactory(CONF_OFFLINE)
    iterator = gen_profiles(10)
    shelled = factory.shell_iterator(iterator, source='some-source')

    assert shelled


def test_the_iterator_can_be_consumed():
    factory = ShellFactory(CONF_OFFLINE)
    iterator = gen_profiles(10)
    shelled = factory.shell_iterator(iterator, destination='some-destination')

    iterator_bis = gen_profiles(10)

    assert list(shelled) == list(iterator_bis)


def test_the_iterator_has_starfish_infos():
    factory = ShellFactory(CONF_OFFLINE)
    iterator = gen_profiles(10)
    shelled = factory.shell_iterator(iterator, source='source')

    assert shelled.starfish
