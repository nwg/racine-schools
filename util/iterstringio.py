# Adapted from
# https://stackoverflow.com/questions/12593576/adapt-an-iterator-to-behave-like-a-file-like-object-in-python

import itertools as it
from io import TextIOBase

class IterStringIO(TextIOBase):
    def __init__(self, iterable=None):
        iterable = iterable or []
        self.iter = it.chain.from_iterable(iterable)

    def not_newline(self, s):
        return s not in {'\n', '\r', '\r\n'}

    def write(self, iterable):
        to_chain = it.chain.from_iterable(iterable)
        self.iter = it.chain.from_iterable([self.iter, to_chain])

    def read(self, n=None):
        return ''.join(it.islice(self.iter, None, n))

    def readline(self, n=None):
        to_read = it.takewhile(self.not_newline, self.iter)
        return ''.join(it.islice(to_read, None, n))
