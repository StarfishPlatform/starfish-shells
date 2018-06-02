from starfish_shell import ShellFactory, Config
from tests.utils import gen_profiles, noop_processing


def test_shell_around_a_simple_functions(mock_starfish):
    config = Config(mock_starfish.url, 'test_1', 'run_1')
    starfish = ShellFactory(config)
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


def test_shell_around_generators(mock_starfish):
    # Arrange
    config = Config(mock_starfish.url, 'test_3', 'run_1')
    starfish = ShellFactory(config)

    def first_3(xs):
        for i, x in enumerate(xs):
            if i < 3:
                yield x

    iterable = gen_profiles(10)

    # Act: Shell Processes
    shelled_input = starfish.shell_iterator(iterable, source='my-shelled-source')
    processed = first_3(shelled_input)
    shelled_result = starfish.shell_iterator(processed, destination='my-shelled-destination')
    result = list(shelled_result)

    # Assert
    assert shelled_input.starfish.consumed == 10
    assert shelled_result.starfish.consumed == 3
