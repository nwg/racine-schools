#!/usr/bin/env python3

import csv
import psycopg2
from psycopg_utils import insert_or_update

conn = psycopg2.connect("dbname='schools' user='postgres' host='localhost' password=''")
cur = conn.cursor()

def get_bool(s):
    try:
        return bool(s)
    except ValueError:
        return False

def format_grade(s):
    try:
        intgrade = int(s)
    except ValueError:
        return s
    return f'{intgrade:02}'

def schools():
    with open('racine-schools-directory.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            d = {}
            d['longname'] = row['name']
            d['is_elementary'] = get_bool(row['E'])
            d['is_middle'] = get_bool(row['M'])
            d['is_high'] = get_bool(row['H'])

            d['low_grade'] = format_grade(row['low grade'])
            d['high_grade'] = format_grade(row['high grade'])

            yield d

insert_or_update(cur, 'schools', ('longname',), schools())

conn.commit()
