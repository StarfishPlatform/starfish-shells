from starfish_shell import Shell, ShellIterator
from starfish_shell import ShellProcess
from tests.utils import gen_profile
from tests.utils import identity


class TestShell:
    def test_shell_lets_you_push_profiles(self):
        profile = gen_profile()
        shell = Shell()
        assert shell.push(profile)

    def test_shell_remembers_pushed_profiles(self):
        profile = gen_profile()
        shell = Shell()
        shell.push(profile)

        assert shell.starfish.consummed == 1


class TestShellProcess:
    def test_shell_is_a_function(self):
        shell = ShellProcess(identity)
        assert shell([1, 2, 3])


class TestShellIterator:
    def test_iterator_is_an_iterator(self):
        shell = ShellIterator([1, 2, 3])
        assert list(shell)
