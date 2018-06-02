from starfish_shell.config import ConfigTestOffline
from starfish_shell.shells import Shell, ShellIterator, ShellProcess, DIR_INPUT, DIR_OUTPUT
from tests.utils import gen_profile, noop_processing, check_log
from tests.utils import identity


class TestShell:
    def test_shell_lets_you_push_profiles(self):
        profile = gen_profile()
        shell = Shell(ConfigTestOffline())
        assert shell.push(profile, direction=DIR_INPUT, storage='some_storage')

    def test_shell_remembers_pushed_profiles(self):
        profile = gen_profile()
        shell = Shell(ConfigTestOffline())
        shell.push(profile, direction=DIR_INPUT, storage='some-storage')

        assert shell.starfish.consumed == 1

    def test_shell_pushes_required_field(self):
        profile = gen_profile()
        conf = ConfigTestOffline()
        shell = Shell(conf)
        shell.push(profile, direction=DIR_INPUT, storage='some-storage')
        shell.push(profile, direction=DIR_OUTPUT, storage='some-storage')

        logs = conf.logs
        assert len(logs) == 2
        assert check_log(logs[0])
        assert check_log(logs[1])


class TestShellProcess:
    def test_is_a_function(self):
        conf = ConfigTestOffline().with_matcher('test_random')
        shell = ShellProcess(identity, conf, source='source', destination='destination')
        assert shell([1, 2, 3])

    def test_output_equals_to_regular_function(self):
        conf = ConfigTestOffline().with_matcher('test_random')
        shell = ShellProcess(identity, conf, source='source', destination='destination')
        assert list(shell([1, 2, 3])) == [1, 2, 3]

    def test_works_with_generator(self):
        conf = ConfigTestOffline().with_matcher('test_random')
        shell = ShellProcess(noop_processing, conf, source='source', destination='destination')
        assert list(shell([1, 2, 3])) == [1, 2, 3]


class TestShellIterator:
    def test_iterator_is_an_iterator(self):
        conf = ConfigTestOffline().with_matcher('test_random')
        shell = ShellIterator([1, 2, 3], conf, source='some-source')
        assert list(shell)
