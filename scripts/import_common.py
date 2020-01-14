import psycopg2.extras
from psycopg2 import sql
import itertools
from collections.abc import Sequence
from functools import reduce
import operator
import logging

def get_csv_str(s, orr=None):
    s = s.strip()
    if s == '':
        return orr
    return s

def get_csv_int2(s, orr=None):
    try:
        return int(s.strip())
    except ValueError:
        pass
    return orr

def get_csv_float(s, orr=None):
    try:
        return float(s)
    except:
        pass
    return orr


def get_csv_float_int(s):
    if s == '':
        return None
    try:
        n = float(s)
        m = int(n)
        if float(m) != n:
            raise(ValueError, f'Not an integer {s}')
        return m
    except ValueError:
        pass
    return s

def lremove(s, prefix):
    if s.find(prefix) != 0:
        raise ValueError(f'{prefix} is not a prefix of {s}')
    return s[len(prefix):]

def get_state_ids(lea, schid):
    lea_split = lea.split('-')
    lea_nostate = ''.join(lea_split[1:])
    schid_nolea = lremove(schid, lea + '-')

    #print(f'split {lea} and {schid} into {lea_nostate}, {schid_nolea}')

    return (lea_nostate, schid_nolea)

