from starfish_shell import Shell, ShellIterator, ConfigTestOffline, DIR_INPUT
from starfish_shell import ShellProcess
from tests.utils import gen_profile, noop_processing
from tests.utils import identity

CONF_OFFLINE = ConfigTestOffline()


class TestShell:
    def test_shell_lets_you_push_profiles(self):
        profile = gen_profile()
        shell = Shell(CONF_OFFLINE)
        assert shell.push(profile, direction=DIR_INPUT, storage='some_storage')

    def test_shell_remembers_pushed_profiles(self):
        profile = gen_profile()
        shell = Shell(CONF_OFFLINE)
        shell.push(profile, direction=DIR_INPUT, storage='some-storage')

        assert shell.starfish.consumed == 1


class TestShellProcess:
    def test_is_a_function(self):
        shell = ShellProcess(identity, CONF_OFFLINE, source='source', destination='destination')
        assert shell([1, 2, 3])

    def test_output_equals_to_regular_function(self):
        shell = ShellProcess(identity, CONF_OFFLINE, source='source', destination='destination')
        assert list(shell([1, 2, 3])) == [1, 2, 3]

    def test_works_with_generator(self):
        shell = ShellProcess(noop_processing, CONF_OFFLINE, source='source', destination='destination')
        assert list(shell([1, 2, 3])) == [1, 2, 3]


class TestShellIterator:
    def test_iterator_is_an_iterator(self):
        shell = ShellIterator([1, 2, 3], CONF_OFFLINE, source='some-source')
        assert list(shell)
