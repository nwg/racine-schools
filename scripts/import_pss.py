#!/usr/bin/env python3

import csv
import itertools
import psycopg2
import logging
import argparse
from import_common import *
from psycopg_utils import insert_or_update
import sys

description = """\
Import PSS survey data

Data compatible with this script available at
https://nces.ed.gov/surveys/pss/pssdata.asp
"""

parser = argparse.ArgumentParser(description=description)
parser.add_argument('year', type=int, help='The lower year of the school year for the directory being imported')
parser.add_argument('filename', help='The CSV appointments file')
args = parser.parse_args()

if args.year == 2017:
    T = lambda s: s
elif args.year == 2015:
    T = lambda s: s.lower()
else:
    raise ValueError(f'please provide a valid column transform for year {args.year}')

if args.year == 2017:
    ENCODING = 'utf-8-sig'
elif args.year == 2015:
    ENCODING = 'windows-1252'
else:
    raise ValueError(f'Please specify an encoding for year {args.year}')

def info():
    with open(args.filename, newline='', encoding=ENCODING) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row[T('PCNTNM')] != 'RACINE':
                continue

            d = {}
            d['ppin'] = row[T('PPIN')]
            d['year'] = args.year
            d['kg_hours'] = row[T('P365')].strip() or None
            d['kg_days_per_week'] = row[T('P370')].strip() or None
            d['is_religious'] = row[T('P430')] == '1'
            orientation = row[T('P440')] or None
            if orientation == '-1':
                orientation = None
            d['religious_orientation'] = orientation
            d['days_in_year'] = row[T('P645')]
            d['hours_in_day'] = row[T('P650')]
            d['minutes_in_day'] = row[T('P655')]
            d['num_students'] = row[T('NUMSTUDS')]
            d['num_fte_teachers'] = row[T('NUMTEACH')]
            d['enrollment'] = row[T('P305')] or None

            yield d

GRADE_MAP = (
    ('PK', T('P150')),
    ('KG', T('P160')),
    ('01', T('P190')),
    ('02', T('P200')),
    ('03', T('P210')),
    ('04', T('P220')),
    ('05', T('P230')),
    ('06', T('P240')),
    ('07', T('P250')),
    ('08', T('P260')),
    ('09', T('P270')),
    ('10', T('P280')),
    ('11', T('P290')),
    ('12', T('P300'))
)

def grade_counts():
    with open(args.filename, newline='', encoding=ENCODING) as csvfile:
        reader = csv.DictReader(csvfile)
        ds = []
        for row in reader:
            if row[T('PCNTNM')] != 'RACINE':
                continue
            for grade, code in GRADE_MAP:
                enrollment = row[code].strip() or None
                if not enrollment:
                    continue
                d = {}
                d['ppin'] = row[T('PPIN')]
                d['year'] = args.year
                d['grade'] = grade
                d['enrollment'] = enrollment
                ds.append(d)
        return ds

DEMOGRAPHIC_MAP = (
    ('american_indian_or_alaska_native', T('P310')),
    ('asian', T('P316')),
    ('hawaiian_or_pacific_islander', T('P318')),
    ('hispanic', T('P320')),
    ('black', T('P325')),
    ('white', T('P330')),
    ('two_or_more_races', T('P332')),
    ('male', T('P340')),
    ('total', T('NUMSTUDS'))
)

def demo_counts():
    with open(args.filename, newline='', encoding=ENCODING) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row[T('PCNTNM')] != 'RACINE':
                continue
            d = {}
            d['ppin'] = row[T('PPIN')]
            d['year'] = args.year
            for col, code in DEMOGRAPHIC_MAP:
                count = row[code].strip() or 0
                d[col] = count
            d['female'] = int(d['total']) - int(d['male'])
            assert int(d['female']) > 0
            yield d

if __name__ == '__main__':

    conn = psycopg2.connect("dbname='schools' user='postgres' host='localhost' password=''")
    cur = conn.cursor()

    insert_or_update(cur, 'pss_info', ('ppin', 'year'), info())
    insert_or_update(cur, 'pss_enrollment_grade_counts', ('ppin', 'year', 'grade'), grade_counts())
    insert_or_update(cur, 'pss_enrollment_demographic_counts', ('ppin', 'year'), demo_counts())

    conn.commit()
