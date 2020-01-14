#!/usr/bin/env python3

import csv
import psycopg2
from import_common import *
import argparse
from psycopg_utils import insert_many, select, idequals
import sys

description = """\
Import CSV OCR discipline data

data available from "detailed data tables" -> discipline section of ocr.ed.gov
"""

parser = argparse.ArgumentParser(description=description)
parser.add_argument('year', type=int, help='The lower year of the school year for the directory being imported')
parser.add_argument('filename', help='The CSV discipline file')
args = parser.parse_args()


def counts():
    with open(args.filename, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row['Lea State'] == '':
                continue

            d = {}
            d['nces_id'] = get_csv_int2(row['ID'])
            year = get_csv_int2(row['Year'])
            if year != args.year:
                raise Exception(f'bad year {year}')
            d['year'] = year

            d['category'] = get_csv_str(row['Category'])
            d['sex'] = get_csv_str(row['Sex'])
            d['american_indian_or_alaska_native'] = get_csv_str(row['American Indian or Alaska Native'])
            d['asian'] = get_csv_float_int(row['Asian'])
            d['hawaiian_or_pacific_islander'] = get_csv_float_int(row['Hawaiian/ Pacific Islander'])
            d['hispanic'] = get_csv_float_int(row['Hispanic'])
            d['black'] = get_csv_float_int(row['Black'])
            d['white'] = get_csv_float_int(row['White'])
            d['two_or_more_races'] = get_csv_float_int(row['Two or more races'])
            d['total_idea_only'] = get_csv_float_int(row['Total (IDEA)'])
            d['total_504_only'] = get_csv_float_int(row['SWD (Section 504 only)'])
            d['total'] = get_csv_float_int(row['Total'])
            d['less_lep'] = get_csv_float_int(row['LEP'])

            yield d



conn = psycopg2.connect("dbname='schools' user='postgres' host='localhost' password=''")
cur = conn.cursor()

query = select('discipline_counts_imported', (sql.Identifier('year'),), where=idequals('year', args.year))
cur.execute(query)
if cur.fetchone():
    print(f'Already imported discipline for year {args.year}')
    sys.exit(0)

insert_many(cur, 'discipline_counts', counts())

query = sql.SQL('insert into discipline_counts_imported values({})').format(sql.Literal(args.year))
cur.execute(query)

conn.commit()
