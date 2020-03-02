#!/usr/bin/env python3

import csv
import itertools
import psycopg2
import logging
import argparse
from import_common import *
from psycopg_utils import insert_many, select, idequals
import sys

description = """\
Import CSV staff appointment info

Newer data compatible with this script available at
https://publicstaffreports.dpi.wi.gov/PubStaffReport/Public/PublicReport/AllStaffReport

Data (old) not compatible with this script available from
https://dpi.wi.gov/cst/data-collections/staff/published-data
"""

parser = argparse.ArgumentParser(description=description)
parser.add_argument('year', type=int, help='The lower year of the school year for the directory being imported')
parser.add_argument('filename', help='The CSV appointments file')
args = parser.parse_args()

RACE_MAP = dict([
    ('W - White', 'W'),
    ('B - Black or African American', 'B'),
    ('H - Hispanic/Latino', 'H'),
    ('A - Asian', 'A'),
    ('T - Two or More Races', 'T'),
    ('I - American Indian or Alaska Native', 'I'),
    ('P - Native Hawaiian or Other Pacific Islander', 'P')
])

def appointments():
    with open(args.filename, newline='') as csvfile:
        csvfile.readline()
        reader = csv.DictReader(csvfile)
        for row in reader:
            '''
            if row['ID Nbr'].strip() == '':
                continue
            if row['Staff Cat'].strip() == '':
                continue
            '''
            if row['School Year'] != f'{args.year + 1}':
                raise Exception('Year does not match file')

            def get_hyphen_dual(s):
                items = s.split(' - ', maxsplit=1)
                if len(items) != 2:
                    return None
                items = [item.strip() for item in items]
                if any(item == '' for item in items):
                    return None
                return items

            work_school = get_hyphen_dual(row['Assignment Work School'])
            if work_school == None:
                continue
            state_school_id, _ = work_school

            work_agency = get_hyphen_dual(row['Assignment Work Agency'])
            if work_agency == None:
                continue
            state_lea_id, _ = work_agency
            
            d = {}

            d['state_school_id'] = state_school_id
            d['state_lea_id'] = state_lea_id

            d['first_name'] = row['First Name'].strip()
            d['last_name'] = row['Last Name'].strip()
            d['race'] = RACE_MAP[row['RaceEthnicity']]

            d['position_category'] = get_csv_str(row['Position Classification']) or 'Other'
            d['gender'] = row['Gender'].strip()

            staff_category, _ = get_hyphen_dual(row['Assignment Staff Category'])
            staff_category = int(staff_category)

            high_degree = get_hyphen_dual(row['Contract High Degree'])
            if high_degree == None:
                education_level = None
            else:
                education_level = int(high_degree[0])

            if education_level not in (3, 4, 5, 6, 7, 8):
                if staff_category in (0, 1):
                    logging.warning(f'Regular or Special Ed instruction staff with unreported or invalid "High Degree" -- code was {education_level}')
                education_level = None

            d['education_level'] = education_level
            d['fte'] = get_csv_float(row['Assignment FTE'])

            d['year'] = args.year

            yield d


if __name__ == '__main__':

    conn = psycopg2.connect("dbname='schools' user='postgres' host='localhost' password=''")
    cur = conn.cursor()

    stmt = select('appointments_imported', (sql.Identifier('year'),), where=idequals('year', args.year))
    cur.execute(stmt)
    if cur.fetchone():
        print(f'Already imported appointments for year {args.year}')
        sys.exit(0)
  
    insert_many(cur, 'appointments', appointments())

    stmt = sql.SQL('insert into appointments_imported values({})').format(sql.Literal(args.year))
    cur.execute(stmt)

    conn.commit()
