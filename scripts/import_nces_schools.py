#!/usr/bin/env python3

import sys
import csv
import psycopg2
import argparse
from import_common import *
from psycopg2 import sql
from collections.abc import Sequence
import itertools as it
from psycopg_utils import insert_many_fast

description = """\
Import CSV schools info
Main data available for download from https://nces.ed.gov/ccd/pubschuniv.asp
Geo data at https://nces.ed.gov/programs/edge/Geographic/SchoolLocations
"""

parser = argparse.ArgumentParser(description=description)
parser.add_argument('year', type=int, help='The lower year of the school year for the directory being imported')
parser.add_argument('directory', help='The CSV schools directory file')
parser.add_argument('characteristics', help='The CSV school characteristics file')
parser.add_argument('lunch', help='The CSV school lunch program eligibility file')
parser.add_argument('geo', help='The CSV school geo file')
parser.add_argument('membership', help='The CSV school membership file')
args = parser.parse_args()

def get_char(s):
    if s in ['M', 'N']:
        return None
    return s


lunch_categories = (
    ('Free lunch qualified', 'FR'),
    ('Reduced-price lunch qualified', 'RE'),
)

unzip = lambda x: list(zip(*x))

def is_valid_lunch_category(s):
    return s in unzip(lunch_categories)[0]

def get_lunch_category(s):
    try:
        return dict(lunch_categories)[s]
    except KeyError:
        pass
    return None

def get_year(s):
    return int(s.split('-')[0])

def get_type(s):
    return dict([
        (1, 'RE'),
        (2, 'SE'),
        (3, 'VO'),
        (4, 'AL'),
    ])[int(s)]

def get_grade(s):
    if s in ['M', '-1', 'N', '-2', '-9']:
        return None
    return s

def get_magnet(s):
    if s == 'Yes': return True
    elif s == 'No': return False
    return None

def get_virtual(s):
    if s == 'FACEVIRTUAL':
        return 'FACE'
    elif s == 'FULLVIRTUAL':
        return 'FULL'
    elif s == 'NOTVIRTUAL':
        return 'NOTV'
    elif s == 'SUPPVIRTUAL':
        return 'SUPP'

    return None

def get_int(s):
    try:
        return int(s)
    except ValueError:
        pass
    return None

conn = psycopg2.connect("dbname='schools' user='postgres' host='localhost' password=''")
cur = conn.cursor()

'''
def keys():
    with open(args.directory, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            d = {}
            d['fips_operating_state'] = int(row['FIPST'])
            state_lea_id, state_school_id = get_state_ids(row['ST_LEAID'], row['ST_SCHID'])
            d['state_lea_id'] = state_lea_id
            d['state_school_id'] = state_school_id
            d['nces_lea_id'] = int(row['LEAID'])
            d['nces_school_id'] = int(row['SCHID'])
            d['nces_id'] = int(row['NCESSCH'])

            yield d

print('inserting school keys')
insert_or_update(cur, 'school_keys', ('fips_operating_state', 'state_lea_id', 'state_school_id'), keys())

cur.execute("""select nces_id, id from school_keys""")
keys_map = dict(cur.fetchall())
print(f'Built keys map with {len(keys_map)} keys')
'''

cur.execute("""\
create temporary table nces_membership_import (
    nces_id bigint,
    grade character(2),
    sex character(2),
    race character(2) check (race in ('WH', 'BL', 'HA', 'AI', 'AS', 'HI', 'TW')),
    indicator character(4) check (indicator in ('REGU', 'GSUB', 'TMIN', 'TTOT')),
    count integer
)""")

def get_grade_membership(s):
    return {
        'Grade 1': '01',
        'Grade 2': '02',
        'Grade 3': '03',
        'Grade 4': '04',
        'Grade 5': '05',
        'Grade 6': '06',
        'Grade 7': '07',
        'Grade 8': '08',
        'Grade 9': '09',
        'Grade 10': '10',
        'Grade 11': '11',
        'Grade 12': '12',
        'Grade 13': '13',
        'Kindergarten': 'KG',
        'Pre-Kindergarten': 'PK',
        'Ungraded': 'UG',
        'Adult Education': 'AE',
    }[s]

def get_sex(s):
    return {
        'Male': 'M',
        'Female': 'F'
    }[s]

def get_race(s):
    return {
        'American Indian or Alaska Native': 'AI',
        'Asian': 'AS',
        'Black or African American': 'BL',
        'Hispanic/Latino': 'HI',
        'Native Hawaiian or Other Pacific Islander': 'HA',
        'Two or more races': 'TW',
        'White': 'WH',
    }[s]

NONE = '\\N'

def membership():
    with open(args.membership, encoding='iso-8859-1', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        rowcount = 0
        for row in reader:
            if row['ST'] != 'WI':
                continue

            #rowcount += 1
            #if rowcount > 100000:
            #    break

            assert get_year(row['SCHOOL_YEAR']) == args.year, f"""Bad year {row['SCHOOL_YEAR']}"""

            d = {}
            d['nces_id'] = int(row['NCESSCH'])

            count = row['STUDENT_COUNT']
            if count == '':
                count = 0
            d['count'] = count

            if row['TOTAL_INDICATOR'] == 'Subtotal 4 - By Grade':
                if row['GRADE'] == 'Not Specified':
                    continue

                d['grade'] = get_grade_membership(row['GRADE'])
                d['sex'] = None
                d['race'] = None
                d['indicator'] = 'GSUB'
            elif row['TOTAL_INDICATOR'] == 'Category Set A - By Race/Ethnicity; Sex; Grade':
                if row['SEX'] == 'Not Specified':
                    continue

                d['grade'] = get_grade_membership(row['GRADE'])
                d['sex'] = get_sex(row['SEX'])
                d['race'] = get_race(row['RACE_ETHNICITY'])
                d['indicator'] = 'REGU'
            elif row['TOTAL_INDICATOR'] == 'Derived - Education Unit Total minus Adult Education Count':
                d['grade'] = None
                d['sex'] = None
                d['race'] = None
                d['indicator'] = 'TMIN'
            elif row['TOTAL_INDICATOR'] == 'Education Unit Total':
                d['grade'] = None
                d['sex'] = None
                d['race'] = None
                d['indicator'] = 'TTOT'
            else:
                continue



            yield d

print('inserting temporary membership data')
insert_many_fast(cur, 'nces_membership_import', membership())


RACES = ('WH', 'BL', 'HA', 'AI', 'AS', 'HI', 'TW')

for race in RACES:
    tablename = sql.Identifier(f'nces_membership_import_{race}')
    cur.execute(sql.SQL("""\
        create temporary table {} (
            nces_id bigint,
            grade character(2),
            sex character(2),
            count integer
        )
    """).format(tablename))
    cur.execute(sql.SQL("""create index on {} (nces_id)""").format(tablename))

print('Inserting into disparate race tables')
for race in RACES:
    tablename = sql.Identifier(f'nces_membership_import_{race}')
    cur.execute(sql.SQL("""insert into {} (nces_id, grade, sex, count) (select nces_id, grade, sex, count from nces_membership_import where indicator='REGU' and race={})""").format(tablename, sql.Literal(race)))

print('inserting nces_enrollment_counts data')

cur.execute("""\
    insert into nces_enrollment_counts (
        nces_id,
        year,
        grade,
        sex,
        american_indian_or_alaska_native,
        asian,
        hawaiian_or_pacific_islander,
        hispanic,
        black,
        white,
        two_or_more_races,
        total
    )
        (select
            ai.nces_id,
            %s,
            ai.grade,
            ai.sex,
            ai.count,
            asn.count,
            ha.count,
            hi.count,
            bl.count,
            wh.count,
            tw.count,
            ai.count + asn.count + ha.count + hi.count + bl.count + wh.count + tw.count

            from "nces_membership_import_AI" as ai
            join "nces_membership_import_AS" as asn using (nces_id, grade, sex)
            join "nces_membership_import_HA" as ha using (nces_id, grade, sex)
            join "nces_membership_import_HI" as hi using (nces_id, grade, sex)
            join "nces_membership_import_BL" as bl using (nces_id, grade, sex)
            join "nces_membership_import_WH" as wh using (nces_id, grade, sex)
            join "nces_membership_import_TW" as tw using (nces_id, grade, sex))
        
        on conflict (nces_id, year, grade, sex) do nothing""", [args.year])

print('verifying counts')
cur.execute("""\
    select s1.nces_id, s1.sum_total, i.count from
        (select nces_id, sum(total :: integer) as sum_total from nces_enrollment_counts group by nces_id, year) as s1
        join nces_membership_import as i on
            i.nces_id = s1.nces_id
            AND i.indicator = 'TTOT'
    where s1.sum_total != i.count
""")

if cur.rowcount > 0:
    print(f'{cur.rowcount} rows have mismatched counts')
    print(f'example: {cur.fetchone()}')

conn.commit()
sys.exit(0)

cur.execute("""\
create temporary table directory (
    school_id integer unique,
    name text not null,
    type character(2),
    low_grade character(2),
    high_grade character(2),
    is_charter boolean
)""")

def entries():
    with open(args.directory, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            d = {}
            nces_id = int(row['NCESSCH'])
            d['school_id'] = keys_map[nces_id]
            d['name'] = row['SCH_NAME']
            d['type'] = get_type(row['SCH_TYPE'])
            d['low_grade'] = get_grade(row['GSLO'])
            d['high_grade'] = get_grade(row['GSHI'])
            d['is_charter'] = row['CHARTER_TEXT'] == 'Yes'
            
            yield d

print('inserting school directory data into temporary table')
insert_many(cur, 'directory', entries())

cur.execute("""\
create temporary table characteristics (
    school_id integer unique,
    is_magnet boolean,
    virtual_type character(4)
)""")

def chars():
    with open(args.characteristics, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            d = {}
            nces_id = int(row['NCESSCH'])
            d['school_id'] = keys_map[nces_id]
            d['is_magnet'] = get_magnet(row['MAGNET_TEXT'])
            d['virtual_type'] = get_virtual(row['VIRTUAL'])

            yield d

print('inserting school characteristics data into temporary table')
insert_many(cur, 'characteristics', chars())

q = """\
    select count(*) from
        (select school_id from directory
        where school_id not in
            (select school_id from characteristics)) as s1
"""

cur.execute(q)
count = cur.fetchone()[0]
print(f'{count} schools are missing characteristics')


cur.execute("""\
create temporary table geo (
    school_id integer unique,
    fips_county text,
    latitude double precision,
    longitude double precision
)""")

def geos():
    with open(args.geo, newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            d = {}
            nces_id = int(row['NCESSCH'])
            try:
                d['school_id'] = keys_map[nces_id]
            except KeyError:
                print(f'{nces_id} not found in keys map, skipping')
                continue
            d['fips_county'] = get_char(row['CNTY'])
            d['latitude'] = row['LAT']
            d['longitude'] = row['LON']

            yield d

print('inserting school geo data into temporary table')
insert_many(cur, 'geo', geos())

q = """select count(*) from (select school_id from directory where school_id not in (select school_id from geo)) as s1"""
cur.execute(q)
count = cur.fetchone()[0]
print(f'{count} schools are missing geo data')


column_pairs = [
    ('id', sql.Identifier('school_id')),
    ('name', sql.Identifier('name')),
    ('type', sql.Identifier('type')),
    ('low_grade', sql.Identifier('low_grade')),
    ('high_grade', sql.Identifier('high_grade')),
    ('is_charter', sql.Identifier('is_charter')),
    ('is_magnet', sql.Identifier('is_magnet')),
    ('virtual_type', sql.Identifier('virtual_type')),
    ('fips_county', sql.Identifier('fips_county')),
    ('latitude', sql.Identifier('latitude')),
    ('longitude', sql.Identifier('longitude')),
    ('is_current', sql.Literal(False)),
    ('is_private', sql.Literal(False)),
]


joins = [
    ('left outer', 'characteristics', using('school_id')),
    ('left outer', 'geo', using('school_id')),
]

sel = select('directory', [ pair[1] for pair in column_pairs ], join=joins)
identifier_cols = [ sql.Identifier(pair[0]) for pair in column_pairs ]
assign_columns = sql.SQL(', ').join(identifier_cols)
updates = [ sql.SQL('{0}=EXCLUDED.{0}').format(col) for col in identifier_cols ]
updates = sql.SQL(', ').join(updates)

print('inserting joined data to schools table')
q = sql.SQL("""insert into {} ({}) ({}) on conflict (id) do update set {}""").format(sql.Identifier('schools'), assign_columns, sel, updates)
cur.execute(q)

def lunches():
    with open(args.lunch, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if not is_valid_lunch_category(row['LUNCH_PROGRAM']):
                continue

            d = {}
            nces_id = int(row['NCESSCH'])
            d['school_id'] = keys_map[nces_id]
            d['year'] = args.year
            d['category'] = get_lunch_category(row['LUNCH_PROGRAM'])
            d['student_count'] = get_int(row['STUDENT_COUNT'])

            yield d

print('inserting lunch counts')
insert_many(cur, 'lunch_counts', lunches(), ignore_conflicts=True)

q = """select count(*) from (select school_id from directory where school_id not in (select distinct school_id from lunch_counts)) as s1"""
cur.execute(q)
count = cur.fetchone()[0]
print(f'{count} schools are missing lunch data')


conn.commit()
