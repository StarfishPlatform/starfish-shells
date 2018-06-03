import logging
from collections import namedtuple, defaultdict

import requests

DIR_INPUT = 'in'
DIR_OUTPUT = 'out'


class Properties:
    def __init__(self):
        self.consumed = 0
        self.failed = False
        self.queried = 0


Query = namedtuple('Query', ['profile', 'direction', 'storage'])


class Shell:
    LOGGER_ID = 'Shell'

    def __init__(self, config):
        self._logger = logging.getLogger(self.LOGGER_ID)
        self.starfish = Properties()
        self._config = config
        self._waiting = []

    def __del__(self):
        if self._waiting:
            self._logger.warning("Shell is being garbage collected without having been finalized!")

    @property
    def waiting(self):
        return [*self._waiting]

    def push(self, profile, direction, storage):
        # KEEP THIS SIMPLE, catch happens in do_push

        # TODO: apply matcher ASAP instead of keeping the full profile
        self._waiting.append(Query(profile, direction, storage))

        if len(self._waiting) >= self._config.batch_size:
            self._do_push()
        return True

    def finalize(self):
        self._do_push()

    def _do_push(self):
        try:
            batches = defaultdict(list)

            for query in self._waiting:
                batches[(query.storage, query.direction)].append(query.profile)

            for (storage, direction), batch in batches.items():
                query = self._config.build_query_for(batch, direction, storage)

                print("QUERY=", query)
                self.starfish.consumed += len(batch)
                self.starfish.queried += 1

                if self._config.is_online:
                    r = requests.post(**query)
                    r.raise_for_status()

            # TODO: take care of errors occuring in the middle of this process
            self._waiting = []
        except Exception as e:
            # TODO: loggin
            self.starfish.failed = True
            self._logger.error("push failed", exc_info=1)


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
        try:
            n = next(self._it)
            # Note: do not add behavior here, we protect users from errors within the `push` method.
            self.push(n, direction=self._direction, storage=self._storage)
            return n
        except StopIteration as e:
            self.finalize()
            raise


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

    def _generator(self, xs, direction, storage, finalize=False):
        for x in xs:
            # Note: do not add behavior here, we protect users from errors within the `push` method.
            self.push(x, direction=direction, storage=storage)
            yield x

        if finalize:
            self.finalize()

    def __call__(self, *args, **kwargs):
        arg1, *rest = args
        new_arg_1 = self._generator(arg1, DIR_INPUT, self._source)

        new_args = [new_arg_1, *rest]
        r = self._f(*new_args, **kwargs)

        return self._generator(r, DIR_OUTPUT, self._destination, finalize=True)
