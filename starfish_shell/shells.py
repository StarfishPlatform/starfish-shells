import logging

import requests

DIR_INPUT = 'in'
DIR_OUTPUT = 'out'


class Properties:
    def __init__(self):
        self.consumed = 0
        self.failed = False


class Shell:
    LOGGER_ID = 'Shell'

    def __init__(self, config):
        self._logger = logging.getLogger(self.LOGGER_ID)
        self.starfish = Properties()
        self._config = config

    def push(self, profile, direction, storage):
        try:
            self.starfish.consumed += 1
            query = self._config.build_query_for(profile, direction, storage)

            if self._config.is_online:
                requests.post(**query)
        except Exception as e:
            # TODO: logging
            self.starfish.failed = True
            self._logger.error("push failed", e)

        return True


class ShellIterator(Shell):
    LOGGER_ID = 'ShellIterator'

    def __init__(self, it, config, source=None, destination=None):
        super().__init__(config)
        self._it = iter(it)

        assert bool(source) != bool(destination), "one of 'destination', 'source' should be set."

        if source:
            self._direction = DIR_INPUT
            self._storage = source
        else:
            self._direction = DIR_OUTPUT
            self._storage = destination

    def __iter__(self):
        return self

    def __next__(self):
        n = next(self._it)
        # Note: do not add behavior here, we protect users from errors within the `push` method.
        self.push(n, direction=self._direction, storage=self._storage)
        return n


class ShellProcess(Shell):
    LOGGER_ID = 'ShellProcess'

    def __init__(self, f, config, source, destination):
        super().__init__(config)

        assert source and destination, 'Process needs source and destination parameters'

        # TODO: implement equivalent to `wraps` over `f`
        # this will keep doc, params description, etc.

        self._f = f
        self._source = source
        self._destination = destination

    def _generator(self, xs, direction, storage):
        for x in xs:
            # Note: do not add behavior here, we protect users from errors within the `push` method.
            self.push(x, direction=direction, storage=storage)
            yield x

    def __call__(self, *args, **kwargs):
        arg1, *rest = args
        new_arg_1 = self._generator(arg1, DIR_INPUT, self._source)

        new_args = [new_arg_1, *rest]
        r = self._f(*new_args, **kwargs)

        return self._generator(r, DIR_OUTPUT, self._destination)
