#!/usr/bin/env python3

import psycopg2
import psycopg2.extras
from psycopg2 import sql
import argparse
import sys
import math
import os
import operator
import re
import traceback
import importlib


description = """\
Update and check the version of the database
"""

parser = argparse.ArgumentParser(description=description)
parser.add_argument('--to-version', type=int, default=math.inf, help='Update to the given version, if needed')
args = parser.parse_args()

try:
    thisdir = os.path.dirname(os.path.realpath(__file__))
except NameError:
    thisdir = os.getcwd()

def start_version(cur):
    query = sql.SQL('''select version from version order by version desc limit 1''')
    cur.execute(query)
    row = cur.fetchone()
    if row == None:
        return 0
    return row['version'] + 1

if __name__ == '__main__':
    conn = psycopg2.connect("dbname='schools' user='postgres' host='localhost' password=''")
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    q = sql.SQL('''
        create table if not exists version (
            version integer unique not null,
            updated_at timestamp default now()
        )
    ''')
    cur.execute(q)

    e = re.compile(r'(?P<module>script_(?P<version>\d{5}))\.py')

    start = start_version(cur)

    files = []
    for file in os.listdir(thisdir):
        m = e.fullmatch(file)
        if m == None:
            continue

        version = int(m.group('version'))
        if version >= start:
            files.append((m, file))

    files.sort(key=operator.itemgetter(1))

    new_version = None

    for m, file in files:
        version = int(m.group('version'))
        new_version = version
        module = m.group('module')
        try:
            print(f'Executing updates from {file}')
            script = importlib.import_module(module)
            script.execute(cur)
        except:
            traceback.print_exc()
            sys.exit(1)

        q = sql.SQL('''insert into version (version) values ({})''').format(sql.Literal(version))
        cur.execute(q)

    if new_version == None:
        print('Already up to date')
    else:
        print(f'Updated database to version {new_version}')

    conn.commit()
