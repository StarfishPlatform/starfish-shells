import requests

DIR_INPUT = 'in'
DIR_OUTPUT = 'out'


class StarfishProperties:
    def __init__(self):
        self.consummed = 0


class Shell:
    def __init__(self, config):
        self.starfish = StarfishProperties()
        self._config = config

    def push(self, profile, direction, storage):
        self.starfish.consummed += 1

        if self._config.is_online:
            query = self._config.build_query_for(profile, direction, storage)
            requests.post(**query)

        return True


class ShellIterator(Shell):
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
        self.push(self._it, direction=self._direction, storage=self._storage)
        return n


class ShellProcess(Shell):
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
            self.push(x, direction=direction, storage=storage)
            yield x

    def __call__(self, *args, **kwargs):
        arg1, *rest = args
        new_arg_1 = self._generator(arg1, DIR_INPUT, self._source)

        new_args = [new_arg_1, *rest]
        r = self._f(*new_args, **kwargs)

        return self._generator(r, DIR_OUTPUT, self._destination)
