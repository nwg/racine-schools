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

Data (old) compatible with this script available from
https://dpi.wi.gov/cst/data-collections/staff/published-data

Newer data not compatible with this script available at
https://publicstaffreports.dpi.wi.gov/PubStaffReport/Public/PublicReport/AllStaffReport
"""

parser = argparse.ArgumentParser(description=description)
parser.add_argument('year', type=int, help='The lower year of the school year for the directory being imported')
parser.add_argument('filename', help='The CSV appointments file')
args = parser.parse_args()

category_map = {
    5: 'Administrators',
    6: 'Administrators',
    8: 'Administrators',
    9: 'Other',
    10: 'Administrators',
    17: 'Administrators',
    18: 'Teachers',
    19: 'Teachers',
    43: 'Other',
    50: 'Pupil Services',
    51: 'Administrators',
    52: 'Administrators',
    53: 'Teachers',
    54: 'Pupil Services',
    55: 'Pupil Services',
    59: 'Pupil Services',
    62: 'Pupil Services',
    63: 'Pupil Services',
    64: 'Other',
    73: 'Other',
    75: 'Pupil Services',
    79: 'Administrators',
    80: 'Administrators',
    83: 'Administrators',
    84: 'Teachers',
    85: 'Other',
    86: 'Teachers',
    87: 'Teachers',
    88: 'Other',
    90: 'Administrators',
    91: 'Administrators',
    92: 'Administrators',
    93: 'Administrators',
    96: 'Aides / Paraprofessionals',
    97: 'Aides / Paraprofessionals',
    98: 'Other',
    99: 'Other'
}

def appointments():
    with open(args.filename, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row['ID Nbr'].strip() == '':
                continue
            if row['Staff Cat'].strip() == '':
                continue
            if row['Year Session'] != f'{args.year + 1}R':
                raise Exception('Year does not match file')

            state_school_id = get_csv_str(row['School Cd'])
            if state_school_id == None:
                continue
            state_lea_id = get_csv_str(row['Work Agncy Cd'])
            if state_lea_id == None:
                continue
            
            d = {}
            d['state_school_id'] = state_school_id
            d['state_lea_id'] = state_lea_id
            d['staff_id'] = get_csv_str(row['File Number'], orr=None)

            d['first_name'] = row['First Name'].strip()
            d['last_name'] = row['Last Name'].strip()
            position_code = int(row['Position Cd'])
            d['position_code'] = position_code
            d['position_category'] = category_map[position_code]
            d['gender'] = row['Gndr']

            staff_category = get_csv_int2(row['Staff Cat'])

            education_level = get_csv_int2(row['High Degree'])

            if education_level not in (3, 4, 5, 6, 7, 8):
                if staff_category in (0, 1):
                    logging.warning(f'Regular or Special Ed instruction staff with unreported or invalid "High Degree" -- code was {education_level}')
                education_level = None

            d['education_level'] = education_level
            fte = get_csv_float(row['Assgn FTE'])
            if fte != None:
                fte = fte / 100.
            d['fte'] = fte

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
