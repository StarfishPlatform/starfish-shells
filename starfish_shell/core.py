import logging

import time

from starfish_shell import Config
from .shells import ShellIterator
from .shells import ShellProcess


def _generate_run_id():
    return str(int(time.time() * 1000))


class ShellFactory:
    def __init__(self, config):
        self._config = config
        self._logger = logging.getLogger('ShellFactory')

    @classmethod
    def from_env(cls, matcher=None):
        config = Config.from_env(matcher=matcher)
        return ShellFactory(config)

    @property
    def config(self):
        return self._config

    def shell_process(self, f, source=None, destination=None):
        return ShellProcess(f, self._config, source=source, destination=destination)

    def shell_iterator(self, it, source=None, destination=None):
        return ShellIterator(it, self._config, source=source, destination=destination)
