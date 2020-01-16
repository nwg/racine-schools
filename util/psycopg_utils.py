import psycopg2.extras
from psycopg2 import sql
import itertools
from collections.abc import Sequence
import logging
from stringcsv import StringDictWriter
from iterstringio import IterStringIO

def colsequal(col1, col2):
    return equals(sql.Identifier(*col1), sql.Identifier(*col2))

def idequals(ident, value):
    return equals(sql.Identifier(ident), sql.Literal(value))

def equals(id1, id2):
    return sql.SQL('=').join([id1, id2])

def andd(elements):
    return sql.SQL('( {} )').format(sql.SQL(' AND ').join(elements))

def orr(elements):
    return sql.SQL('( {} )').format(sql.SQL(' OR ').join(elements))


def on(sq):
    return sql.SQL('ON {}').format(sq)

def using(columns):
    if not isinstance(columns, Sequence) or isinstance(columns, str):
        columns = (columns,)

    columns_sql = sql.SQL(', ').join(sql.Identifier(column) for column in columns)
    return sql.SQL('using ({})').format(columns_sql)

def select(table, items, join=[], where=None):
    if items == '*':
        columns_list = sql.SQL('*')
    else:
        columns_list = sql.SQL(', ').join(items)
    q = sql.SQL("""select {} from {}""").format(columns_list, sql.Identifier(table))
    if join:
        for jtype, jtable, condition in join:
             q = sql.SQL('{} {} join {} {}').format(q, sql.SQL(jtype), sql.Identifier(jtable), condition)
    if where:
        q = sql.SQL('{} where {}').format(q, where)

    return q


NULL = ''

def insert_many_fast(cur, table, ds):
    ds = iter(ds)
    try:
        first = next(ds)
    except StopIteration:
        return
    ds = itertools.chain([first], ds)
    keys = sorted(first.keys())

    def csvout():
        writer = StringDictWriter(fieldnames=keys)
        for d in ds:
            dnew = {}
            for key, value in d.items():
                if value == None:
                    dnew[key] = NULL
                else:
                    dnew[key] = value
            d = dict(dnew)
            yield writer.getrow(d)

    iso = IterStringIO(csvout())

    cols = sql.SQL(', ').join(sql.Identifier(col) for col in keys)

    cmd = sql.SQL("""COPY {} ({}) FROM STDIN WITH (FORMAT CSV, DELIMITER ',')""").format(sql.Identifier(table), cols)
    print(f'''Inserting with cmd {sql.SQL('').join(cmd)}''')
    cur.copy_expert(cmd, iso)


def insert_many(cur, table, ds, ignore_conflicts=False):
    ds = iter(ds)
    try:
        first = next(ds)
    except StopIteration:
        return

    keys = sorted(first.keys())
    logging.debug(f'inserting {keys}')
    q = f"""INSERT INTO {table} ({','.join(keys)}) VALUES %s"""
    if ignore_conflicts:
        q = q + """ON CONFLICT DO NOTHING"""
    def values():
        for d in itertools.chain([first], ds):
            assert sorted(d.keys()) == keys, f'Mismatched keys {d.keys()}'
            yield [ d[key] for key in keys ]
    psycopg2.extras.execute_values(cur, q, values(), template=None, page_size=100)

def insert_or_update(cur, table, match_keys, ds):
    ds = iter(ds)
    try:
        first = next(ds)
    except StopIteration:
        return
    keys = sorted(first.keys())
    logging.debug(f'insert_or_update {table}: {keys}')

    columns = sql.SQL(', ').join(sql.Identifier(key) for key in keys)
    match_identifiers = sql.SQL(', ').join( sql.Identifier(key) for key in match_keys )
    updates = sql.SQL(', ').join(sql.SQL('{0}=EXCLUDED.{0}').format(sql.Identifier(key)) for key in keys)
    q = sql.SQL("""INSERT INTO {} ({}) VALUES %s ON CONFLICT ({}) DO UPDATE SET {}""").format(sql.Identifier(table), columns, match_identifiers, updates)

    def values():
        for d in itertools.chain([first], ds):
            assert sorted(d.keys()) == keys, f'Mismatched keys {d.keys()}'
            yield [ d[key] for key in keys ]

    psycopg2.extras.execute_values(cur, q, values(), template=None, page_size=100)

def update_many(cur, table, update_keys, ds, fetch_updated_keys=False):
    ds = iter(ds)
    try:
        first = next(ds)
    except StopIteration:
        return [] if fetch_updated_keys else None

    keys = sorted(first.keys())
    updates = [ sql.SQL('{0}=upd.{0}').format(sql.Identifier(key)) for key in keys ]
    updates = sql.SQL(', ').join(updates)
    keys_separated = sql.SQL(', ').join([sql.Identifier(key) for key in keys])
    conditions = [ sql.SQL('{0}.{1}=upd.{1}').format(sql.Identifier(table), sql.Identifier(key)) for key in update_keys ]
    conditions = sql.SQL(' AND ').join(conditions)
    returning = [ sql.SQL('{0}.{1}').format(sql.Identifier(table), sql.Identifier(key)) for key in update_keys ]
    returning = sql.SQL(', ').join(returning)
    q = sql.SQL("""UPDATE {} SET {} FROM (VALUES %s) as upd({}) WHERE {} RETURNING {}""").format(sql.Identifier(table), updates, keys_separated, conditions, returning)

    def values():
        for d in itertools.chain([first], ds):
            assert sorted(d.keys()) == keys, f'Mismatched keys {d.keys()}'
            yield [ d[key] for key in keys ]
    
    return psycopg2.extras.execute_values(cur, q, values(), template=None, page_size=100, fetch=fetch_updated_keys)
