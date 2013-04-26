'''A way to freeze time'''


class HammerTime(object):
    '''A way to freeze time'''
    def __init__(self):
        import time
        self._timef = time.time
        self._time = time
        self._frozen = False
        self._when = None

    def __enter__(self):
        '''Freeze time for the duration of this context'''
        self.freeze()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.thaw()

    def freeze(self):
        '''Stop time at the current time'''
        self._frozen = True
        self._when = self._timef()

    def thaw(self):
        '''Melt the frozen time'''
        self._frozen = False

    def frozen(self):
        '''Return whether or not we're currently frozen'''
        return self._frozen

    def advance(self, increment):
        '''Skip ahead a bit in time'''
        self._when += increment

    def time(self):
        '''Return the current timestamp'''
        return (self._frozen and self._when) or self._timef()

    def __iadd__(self, increment):
        self.advance(increment)
        return self

    def __isub__(self, increment):
        self.advance(-increment)
        return self

    def __getattr__(self, key):
        return getattr(self._time, key)


def patch():
    '''Monkey-patch the time module'''
    obj = HammerTime()
    import time
    for key in ['__enter__', '__exit__', 'freeze', 'thaw', 'frozen', 'advance',
        'time', '__iadd__', '__isub__', '__getattr__']:
        setattr(time, key, getattr(obj, key))
