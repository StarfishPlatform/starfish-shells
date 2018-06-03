from starfish_shell import ShellFactory, Config
from tests.utils import gen_profiles, noop_processing, env, check_server_log


def first_3(xs):
    for i, x in enumerate(xs):
        if i < 3:
            yield x


def test_shell_around_a_simple_functions(mock_starfish):
    with env(STARFISH_API_URL=mock_starfish.url,
             STARFISH_SERVICE_ID='test_1',
             STARFISH_RUN_ID='run_1'):
        starfish = ShellFactory.from_env()
        shelled = starfish.shell_process(
            noop_processing,
            source='some-source-identifier',
            destination='some-destination-identifier'
        )

        # that would be the moment where you store profiles somewhere else.
        list(shelled(gen_profiles(10)))

        # The content has been consumed
        assert shelled.starfish.consumed == 20

        # The server has been requested
        logs = mock_starfish.logs
        assert len(logs) == 20
        assert 'some-source-identifier' in logs.sources
        assert 'some-destination-identifier' in logs.destinations

        for log in logs:
            assert check_server_log(log)


def test_shell_around_generators(mock_starfish):
    # Arrange
    config = Config(mock_starfish.url, 'test_3', 'run_1')
    starfish = ShellFactory(config)

    iterable = gen_profiles(10)

    # Act: Shell Processes
    shelled_input = starfish.shell_iterator(iterable, source='my-shelled-source')
    processed = first_3(shelled_input)
    shelled_result = starfish.shell_iterator(processed, destination='my-shelled-destination')
    list(shelled_result)  # Consume the result iterator

    # Assert
    assert shelled_input.starfish.consumed == 10
    assert shelled_result.starfish.consumed == 3


def test_some_failure_should_never_block_the_processing():
    # Arrange
    config = Config('http://wrong_url', 'test_3', 'run_1')
    starfish = ShellFactory(config)

    iterable = gen_profiles(10)

    # Act: Shell Processes
    shelled_input = starfish.shell_iterator(iterable, source='my-shelled-source')
    processed = first_3(shelled_input)
    shelled_result = starfish.shell_iterator(processed, destination='my-shelled-destination')
    result = list(shelled_result)  # Consume the result iterator

    # Assert
    assert len(result) == 3
    assert shelled_input.starfish.failed
    assert shelled_result.starfish.failed
