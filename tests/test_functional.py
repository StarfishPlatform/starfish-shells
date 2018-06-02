from starfish_shell import ShellFactory
from tests.utils import gen_profiles


def test_shell_around_a_simple_functions():
    # The function we are wrapping
    def noop_processing(profiles):
        for profile in profiles:
            yield profile

    starfish = ShellFactory()
    shelled = starfish.shell_process(noop_processing)

    list(shelled(gen_profiles(100)))

    assert shelled.starfish.consummed == 100


def test_shell_around_generators():
    # Arrange
    starfish = ShellFactory()

    def first_3(xs):
        for i, x in enumerate(xs):
            if i < 3:
                yield x

    iterable = gen_profiles(10)

    # Act: Shell Processes
    shelled_input = starfish.shell_iterator(iterable)
    processed = first_3(shelled_input)
    shelled_result = starfish.shell_iterator(processed)
    result = list(shelled_result)

    # Assert
    assert shelled_input.starfish.consummed == 10
    assert shelled_result.starfish.consummed == 3
