from .shells import ShellIterator
from .shells import ShellProcess


class ShellFactory:
    def shell_process(self, f):
        return ShellProcess(f)

    def shell_iterator(self, it):
        return ShellIterator(it)
