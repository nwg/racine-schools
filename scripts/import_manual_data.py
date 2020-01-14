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
    with open('data/racine-schools-directory.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if not row['name']:
                continue

            d = {}
            d['longname'] = row['name']
            d['is_elementary'] = get_bool(row['E'])
            d['is_middle'] = get_bool(row['M'])
            d['is_high'] = get_bool(row['H'])

            d['low_grade'] = format_grade(row['low grade'])
            d['high_grade'] = format_grade(row['high grade'])
            d['state_lea_id'] = row['District Code'] or None
            d['state_school_id'] = row['School Code'] or None
            d['pss_ppin'] = row['PPIN'] or None
            d['nces_id'] = row['NCES'] or None

            yield d

insert_or_update(cur, 'schools', ('longname',), schools())

conn.commit()
