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

def info():
    with open(args.filename, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row['PCNTNM'] != 'RACINE':
                continue

            d = {}
            d['ppin'] = row['PPIN']
            d['year'] = args.year
            d['kg_hours'] = row['P365'].strip() or None
            d['kg_days_per_week'] = row['P370'].strip() or None
            d['is_religious'] = row['P430'] == '1'
            orientation = row['P440']
            if orientation == '-1':
                orientation = None
            d['religious_orientation'] = orientation
            d['days_in_year'] = row['P645']
            d['hours_in_day'] = row['P650']
            d['minutes_in_day'] = row['P655']
            d['num_students'] = row['NUMSTUDS']
            d['num_fte_teachers'] = row['NUMTEACH']
            d['enrollment'] = row['P305'] or None

            print(f"""yielding {d['ppin']}""")

            yield d

GRADE_MAP = (
    ('PK', 'P150'),
    ('KG', 'P160'),
    ('01', 'P190'),
    ('02', 'P200'),
    ('03', 'P210'),
    ('04', 'P220'),
    ('05', 'P230'),
    ('06', 'P240'),
    ('07', 'P250'),
    ('08', 'P260'),
    ('09', 'P270'),
    ('10', 'P280'),
    ('11', 'P290'),
    ('12', 'P300')
)

def grade_counts():
    with open(args.filename, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        ds = []
        for row in reader:
            if row['PCNTNM'] != 'RACINE':
                continue
            for grade, code in GRADE_MAP:
                enrollment = row[code].strip() or None
                if not enrollment:
                    continue
                d = {}
                d['ppin'] = row['PPIN']
                d['year'] = args.year
                d['grade'] = grade
                d['enrollment'] = enrollment
                ds.append(d)
        return ds

if __name__ == '__main__':

    conn = psycopg2.connect("dbname='schools' user='postgres' host='localhost' password=''")
    cur = conn.cursor()

    insert_or_update(cur, 'pss_info', ('ppin', 'year'), info())
    insert_or_update(cur, 'pss_enrollment_grade_counts', ('ppin', 'year', 'grade'), grade_counts())

    conn.commit()
