from starfish_shell.config import ConfigTestOffline
from starfish_shell.shells import Shell, ShellIterator, ShellProcess, DIR_INPUT, DIR_OUTPUT
from tests.utils import gen_profile, noop_processing, check_config_log, random_failure_matcher
from tests.utils import identity, random_matcher


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
        shell.finalize()

        logs = conf.logs
        assert len(logs) == 2
        assert check_config_log(logs[0])
        assert check_config_log(logs[1])

    def test_shell_pushes_per_batches(self):
        profiles = [gen_profile() for _ in range(10)]
        conf = ConfigTestOffline(batch_size=5)
        shell = Shell(conf)

        for profile in profiles:
            shell.push(profile, direction=DIR_OUTPUT, storage='some-storage')

        shell.finalize()

        assert len(shell.waiting) == 0
        assert shell.starfish.queried == 2
        assert shell.starfish.consumed == 10


class TestShellProcess:
    def test_is_a_function(self):
        conf = ConfigTestOffline().with_matcher(random_matcher)
        shell = ShellProcess(identity, conf, source='source', destination='destination')
        assert shell([1, 2, 3])

    def test_output_equals_to_regular_function(self):
        conf = ConfigTestOffline().with_matcher(random_matcher)
        shell = ShellProcess(identity, conf, source='source', destination='destination')
        assert list(shell([1, 2, 3])) == [1, 2, 3]

    def test_works_with_generator(self):
        conf = ConfigTestOffline().with_matcher(random_matcher)
        shell = ShellProcess(noop_processing, conf, source='source', destination='destination')
        assert list(shell([1, 2, 3])) == [1, 2, 3]

    def test_matcher_failure_wont_block_result(self):
        conf = ConfigTestOffline().with_matcher(random_failure_matcher)
        shell = ShellProcess(noop_processing, conf, source='source', destination='destination')

        in_ = list(range(200))
        assert list(shell(in_)) == in_
        assert shell.starfish.failed


class TestShellIterator:
    def test_iterator_is_an_iterator(self):
        conf = ConfigTestOffline().with_matcher(random_matcher)
        shell = ShellIterator([1, 2, 3], conf, source='some-source')
        assert list(shell)
