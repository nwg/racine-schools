#!/usr/bin/env python3

import csv
import psycopg2
from psycopg_utils import insert_or_update

CURRENT_SUMMARY_YEAR = 2018
STAR_SUMMARY_YEAR = 2017

def get_bool(s):
    try:
        return bool(s)
    except ValueError:
        return False

def get_maybe(s):
    if s.lower() == 'not available':
        return None
    if s.strip() == '':
        return None
    return s

def strip_star(s):
    if s == None:
        return s
    return s.rstrip('*')

def has_star(s):
    if s == None:
        return False
    return s.find('*') != -1

def format_grade(s):
    try:
        intgrade = int(s)
    except ValueError:
        return s
    return f'{intgrade:02}'

from urllib.parse import urljoin
LOGO_BASE = 'https://racineschools.s3.amazonaws.com/logos-scaled/'

def logo_url(s):
    if not s: return None
    return urljoin(LOGO_BASE, s)

REPORT_BASE = 'https://racineschools.s3.amazonaws.com/report-cards/'
def report_url(s):
    if s == None: return None
    return urljoin(REPORT_BASE, s)

def get_schools():
    schools = []
    state_school_id_history = []
    nces_id_history = []
    with open('data/racine-schools-directory.csv', newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if not row['id']:
                continue

            d = {}
            school_id = int(row['id'])
            d['id'] = school_id
            d['longname'] = row['name'].strip()
            d['is_elementary'] = get_bool(row['E'])
            d['is_middle'] = get_bool(row['M'])
            d['is_high'] = get_bool(row['H'])
            d['is_private'] = get_bool(row['Private'])

            d['low_grade'] = format_grade(row['low grade'])
            d['high_grade'] = format_grade(row['high grade'])

            assert 0 < len(d['low_grade']) <=2, f"""bad low grade {d['low_grade']} for school {d['longname']}"""
            assert 0 < len(d['high_grade']) <=2, f"""bad high grade {d['high_grade']} for school {{d['longname']}}"""

            d['state_lea_id'] = row['District Code'] or None
            d['state_school_id'] = row['School Code'] or None
            d['pss_ppin'] = row['PPIN'] or None
            d['nces_id'] = row['NCES'] or None
            d['address1'] = row['Address1'] or None
            d['address2'] = row['Address2'] or None
            d['address_comment'] = row['AddressGrades'] or None
            d['phone'] = row['Phone'] or None
            d['second_address1'] = row['SecondAddress1'] or None
            d['second_address2'] = row['SecondAddress2'] or None
            d['second_address_comment'] = row['SecondAddressGrades'] or None
            d['second_phone'] = row['SecondPhone'] or None
            d['website'] = row['Website'] or None
            d['mission'] = row['Mission'] or None
            d['report1'] = report_url(get_maybe(row['Report1'])) or None
            d['report2'] = report_url(get_maybe(row['Report2'])) or None
            d['affiliation'] = row['Affiliation'] or None
            d['logo'] = logo_url(row['Logo'])
            d['disadvantaged_pct'] = strip_star(get_maybe(row['Economically Disadvantaged'])) or None
            d['curriculum_focus'] = row['Curriculum Focus'] or None
            d['num_students'] = strip_star(get_maybe(row['Num Students'])) or None
            d['choice_students_pct'] = strip_star(strip_star(get_maybe(row['Percent Choice Students']))) or None

            starred_data = has_star(row['Economically Disadvantaged']) \
                        or has_star(row['Num Students']) \
                        or has_star(row['Percent Choice Students'])

            d['summary_year'] = STAR_SUMMARY_YEAR if starred_data else CURRENT_SUMMARY_YEAR
            d['is_old_siena'] = row['Affiliation'].find('Siena Schools') != -1

            schools.append(d)


            def parse_values(values):
                if not values:
                    return None
                items = values.split(',')
                return [ item.split(':') for item in items ]

            nces_pairs = parse_values(row['NCES History'])
            if nces_pairs:
                for pair in nces_pairs:
                    end_year, nces_id = pair
                    nces = {}
                    nces['school_id'] = school_id
                    nces['nces_id'] = nces_id
                    nces['end_year'] = end_year
                    nces_id_history.append(nces)

            state_school_id_pairs = parse_values(row['School Code History'])
            if state_school_id_pairs:
                for pair in state_school_id_pairs:
                    end_year, state_state_school_id = pair
                    state_school_id = {}
                    state_school_id['school_id'] = school_id
                    state_school_id['state_school_id'] = state_state_school_id
                    state_school_id['end_year'] = end_year
                    state_school_id_history.append(state_school_id)

    return schools, state_school_id_history, nces_id_history

if __name__  == '__main__':
    conn = psycopg2.connect("dbname='schools' user='postgres' host='localhost' password=''")
    cur = conn.cursor()

    schools, state_school_id_history, nces_id_history = get_schools()

    insert_or_update(cur, 'schools', ('longname',), schools)
    insert_or_update(cur, 'state_school_id_history', ('school_id', 'state_school_id'), state_school_id_history)
    insert_or_update(cur, 'nces_id_history', ('school_id', 'nces_id'), nces_id_history)

    conn.commit()
