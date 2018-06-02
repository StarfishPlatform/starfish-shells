from starfish_shell import ShellFactory
from tests.utils import gen_profiles, identity


def test_i_can_create_the_factory():
    factory = ShellFactory()
    assert factory is not None


def test_i_can_wrap_a_function_with_the_factory():
    factory = ShellFactory()
    shelled = factory.shell_process(identity)
    assert shelled is not None


def test_the_function_i_wrap_doesnt_change_input_outputs():
    factory = ShellFactory()
    shelled = factory.shell_process(identity)
    assert shelled([1, 2, 3]) == [1, 2, 3]


def test_the_result_has_starfish_details():
    factory = ShellFactory()
    shelled = factory.shell_process(identity)
    assert shelled.starfish


def test_i_can_wrap_iterators():
    factory = ShellFactory()
    iterator = gen_profiles(10)
    shelled = factory.shell_iterator(iterator)

    assert shelled


def test_the_iterator_can_be_consummed():
    factory = ShellFactory()
    iterator = gen_profiles(10)
    shelled = factory.shell_iterator(iterator)

    iterator_bis = gen_profiles(10)

    assert list(shelled) == list(iterator_bis)


def test_the_iterator_has_starfish_infos():
    factory = ShellFactory()
    iterator = gen_profiles(10)
    shelled = factory.shell_iterator(iterator)

    assert shelled.starfish
