class StarfishProperties:
    def __init__(self):
        self.consummed = 0


class Shell:
    def __init__(self):
        self.starfish = StarfishProperties()

    def push(self, profile):
        self.starfish.consummed += 1
        return True


class ShellIterator(Shell):
    def __init__(self, it):
        super().__init__()
        self._it = iter(it)

    def __iter__(self):
        return self

    def __next__(self):
        n = next(self._it)
        self.push(self._it)
        return n


class ShellProcess(Shell):
    def __init__(self, f):
        super().__init__()
        self._f = f

    def __call__(self, *args, **kwargs):
        arg1, *rest = args

        for x in arg1:
            self.push(x)

        r = self._f(*args, **kwargs)

        for x in r:
            self.push(x)

        return r
