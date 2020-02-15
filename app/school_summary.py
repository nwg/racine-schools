from flask import render_template
from app import app
from app import db
from app.db import conn, cur
from psycopg_utils import select, idequals, andd, orr, on, colsequal
from psycopg2 import sql
from itertools import repeat

MOST_RECENT_STAFF_YEAR = 2018
MOST_RECENT_PSS_YEAR = 2017
MOST_RECENT_NCES_YEAR = 2017
MOST_RECENT_OCR_YEAR = 2015

RACE_COLS = [
    'american_indian_or_alaska_native',
    'asian',
    'hawaiian_or_pacific_islander',
    'hispanic',
    'black',
    'white',
    'two_or_more_races',
    'total'
]

EDUCATION_MAP = {
    3: 'associate',
    4: 'bachelors',
    5: 'masters',
    6: 'six_year_specialists',
    7: 'doctorate',
    8: 'other',
    None: 'unreported',
}

STAFF_EDUCATION_LEVELS = [
    'associate',
    'bachelors',
    'masters',
    'six_year_specialists',
    'doctorate',
    'other'
]

STAFF_CATEGORIES = (
    'Teachers',
    'Administrators',
    'Aides / Paraprofessionals',
    'Pupil Services',
    'Other',
)

GRADES_ORDER = ('K3', 'K4', 'K5', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13')

NCES_GRADE_MAP = dict([
    ('PK', 'K4'),
    ('KG', 'K5')
])

PSS_GRADE_MAP = dict([
    ('KG', 'K5')
])

def convert_grade(grade):
    return int(grade) if grade.isdigit() else grade

def convert_nces_grade(grade):
    grade = convert_grade(grade)
    if grade in NCES_GRADE_MAP:
        return NCES_GRADE_MAP[grade]
    return grade

def convert_pss_grade(grade):
    grade = convert_grade(grade)
    if grade in PSS_GRADE_MAP:
        return PSS_GRADE_MAP[grade]
    return grade

def get_grades(low_grade, high_grade):
    start_idx = GRADES_ORDER.index(low_grade)
    end_idx = GRADES_ORDER.index(high_grade)

    grades = GRADES_ORDER[start_idx:end_idx+1]
    return [ convert_grade(grade) for grade in grades ]

def pssdict(**extra):
    d = {
        'source': 'pss',
        'url': PSS_SURVEY_WEBSITE,
        'year': MOST_RECENT_PSS_YEAR
    }
    d.update(extra)
    return d

def render_school_summary_with_name(name):
    school = db.school_with_name(name)
    school = dict(school)
    state_lea_id = school['state_lea_id']
    state_school_id = school['state_school_id']
    nces_id = school['nces_id']
    ppin = school['pss_ppin']

    assert state_lea_id in (None, '4620', '8110'), 'Unrecognized Racine LEA ID'
    school['district_name'] = 'Racine Unified' if state_lea_id == '4620' else '21st Century Preparatory School'

    tables = {}
    missing = {}

    tables['summary'] = {}
    missing['summary'] = {}
    if ppin != None:
        pss_info = get_pss_info(cur, MOST_RECENT_PSS_YEAR, ppin)
        if pss_info != None:
            tables['summary']['pss_info'] = pssdict(table=pss_info)
        else:
            missing['summary']['pss_info'] = pssdict()
    else:
        missing['summary']['pss_info'] = pssdict()

    t, m = student_tables(school)
    tables['student'] = t
    missing['student'] = m

    t, m = staff_tables(state_lea_id, state_school_id)
    tables['staff'] = t
    missing['staff'] = m

    t, m = discipline_tables(nces_id)
    tables['discipline'] = t
    missing['discipline'] = m

    return render_template(
        'school.html',
        school=school,
        tables=tables,
        missing=missing,
        grade_order=GRADES_ORDER
    )

DPI_STAFF_WEBSITE = "https://publicstaffreports.dpi.wi.gov/PubStaffReport/Public/PublicReport/AllStaffReport"
PSS_SURVEY_WEBSITE = "https://nces.ed.gov/surveys/pss/pssdata.asp"
NCES_SCHOOL_WEBSITE = "https://nces.ed.gov/ccd/pubschuniv.asp"
OCR_WEBSITE = "https://ocrdata.ed.gov/"

def discipline_tables(nces_id):
    tables = {}
    missing = {}

    def ocrdict(**extra):
        ocr = {
            'source': 'ocr',
            'url': OCR_WEBSITE,
            'year': MOST_RECENT_OCR_YEAR,
        }
        ocr.update(extra)
        return ocr

    if nces_id == None:
        missing['discipline_by_type_by_sex'] = ocrdict()
        missing['discipline_by_type_by_race'] = ocrdict()
    else:
        discipline_by_type_by_sex = get_discipline_by_type_by_sex(cur, MOST_RECENT_OCR_YEAR, nces_id)
        if discipline_by_type_by_sex != None:
            tables['discipline_by_type_by_sex'] = ocrdict(table=discipline_by_type_by_sex)
        else:
            missing['discipline_by_type_by_sex'] = ocrdict()

        discipline_by_type_by_race = get_discipline_by_type_by_race(cur, MOST_RECENT_OCR_YEAR, nces_id)
        if discipline_by_type_by_race != None:
            tables['discipline_by_type_by_race'] = ocrdict(table=discipline_by_type_by_race)
        else:
            missing['discipline_by_type_by_race'] = ocrdict()

    return tables, missing


def student_tables(school):
    is_private = school['is_private']
    nces_id = school['nces_id']
    ppin = school['pss_ppin']
    low_grade = school['low_grade']
    high_grade = school['high_grade']
    grades = get_grades(low_grade, high_grade)

    tables = {}
    missing = {}

    if ppin:
        tables['pss_enrollment_by_grade'] = pssdict(table=get_pss_enrollment_by_grade(cur, MOST_RECENT_PSS_YEAR, ppin), grades=grades)
        tables['pss_enrollment_by_demographic'] = pssdict(table=get_pss_enrollment_by_demographic(cur, MOST_RECENT_PSS_YEAR, ppin))
    elif is_private:
        missing['pss_enrollment_by_grade'] = pssdict()
        missing['pss_enrollment_by_demographic'] = pssdict()

    def ncesdict(**extra):
        d = {
            'source': 'nces',
            'url': NCES_SCHOOL_WEBSITE,
            'year': MOST_RECENT_NCES_YEAR
        }
        d.update(extra)
        return d

    if nces_id == None:
        if not is_private:
            missing['enrollment_by_sex_by_grade'] = ncesdict()
            missing['enrollment_by_grade_by_race'] = ncesdict()
    else:
        enrollment_by_sex_by_grade = get_enrollment_by_sex_by_grade(cur, MOST_RECENT_NCES_YEAR, school)
        if enrollment_by_sex_by_grade == None:
            missing['enrollment_by_sex_by_grade'] = ncesdict()
        else:
            tables['enrollment_by_sex_by_grade'] = ncesdict(table=enrollment_by_sex_by_grade, grades=grades)

        enrollment_by_grade_by_race = get_enrollment_by_grade_by_race(cur, MOST_RECENT_NCES_YEAR, school)
        if enrollment_by_grade_by_race == None:
            missing['enrollment_by_grade_by_race'] = ncesdict()
        else:
            tables['enrollment_by_grade_by_race'] = ncesdict(table=enrollment_by_grade_by_race, grades=grades)

    return tables, missing

def staff_tables(state_lea_id, state_school_id):
    tables = {}
    missing = {}

    def dpidict(**extra):
        d = {
            'source': 'dpi',
            'url': DPI_STAFF_WEBSITE,
            'year': MOST_RECENT_STAFF_YEAR
        }
        d.update(extra)
        return d

    if not state_lea_id or not state_school_id:
        missing['staff_by_sex_by_category'] = dpidict()
        missing['staff_by_category_by_education'] = dpidict()
        missing['staff_by_category_by_tenure'] = dpidict()

        return tables, missing

    staff_by_sex_by_category = get_staff_by_sex_by_category(cur, MOST_RECENT_STAFF_YEAR, state_lea_id, state_school_id)
    if staff_by_sex_by_category == None:
        missing['staff_by_sex_by_category'] = dpidict()
    else:
        tables['staff_by_sex_by_category'] = dpidict(table=staff_by_sex_by_category)

    staff_by_category_by_education = get_staff_by_category_by_education(cur, MOST_RECENT_STAFF_YEAR, state_lea_id, state_school_id)
    if staff_by_category_by_education != None:
        tables['staff_by_category_by_education'] = dpidict(table=staff_by_category_by_education)
    else:
        missing['staff_by_category_by_education'] = dpidict()

    tables['staff_by_category_by_tenure'] = dpidict(table=get_staff_by_category_by_tenure(cur, state_lea_id, state_school_id))

    return tables, missing

def get_pss_info(cur, year, ppin):
    def query_pss_info():
        joins = [
            (
                'left outer',
                'pss_religious_orientation',
                on(
                    colsequal(
                        ['pss_info', 'religious_orientation'],
                        ['pss_religious_orientation', 'category']))
            ),
        ]
        return select('pss_info', '*', where=andd([idequals('ppin', ppin), idequals('year', year)]), join=joins)
    q = query_pss_info()
    cur.execute(q)

    return cur.fetchone()

def get_pss_enrollment_by_grade(cur, year, ppin):
    def query_pss_enrollment():
        where = andd([idequals('year', year), idequals('ppin', ppin)])
        return select('pss_enrollment_grade_counts', (sql.Identifier('grade'), sql.Identifier('enrollment')), where=where)
    q = query_pss_enrollment()
    cur.execute(q)
    enrollment_by_grade = {}
    for row in cur.fetchall():
        enrollment_by_grade[convert_pss_grade(row['grade'])] = row['enrollment']

    return enrollment_by_grade

def get_pss_enrollment_by_demographic(cur, year, ppin):
    def query_pss_enrollment():
        where = andd([idequals('year', year), idequals('ppin', ppin)])
        return select('pss_enrollment_demographic_counts', '*', where=where)
    q = query_pss_enrollment()
    cur.execute(q)

    return cur.fetchone()

def get_enrollment_by_sex_by_grade(cur, year, school):
    nces_id = school['nces_id']
    low_grade = school['low_grade']
    high_grade = school['high_grade']

    check_where = andd([
        idequals('year', year),
        idequals('nces_id', nces_id)
    ])
    query = select('nces_enrollment_counts', '*', where=check_where)
    cur.execute(query)
    if not cur.fetchone():
        return None

    def query_sex_enrollment_counts():
        table = sql.Identifier('nces_enrollment_counts')

        where = andd([
            idequals('year', year),
            idequals('nces_id', nces_id)
        ])

        query = sql.SQL(
            """select grade, sex, sum(total::integer) as total from {} where {} group by grade, sex"""
        ).format(table, where)

        return query

    grades = get_grades(low_grade, high_grade)
    enrollment_by_sex_by_grade = {}
    for sex in 'M', 'F':
        d = {}
        for grade in grades:
            d[grade] = 0
        d['total'] = 0
        enrollment_by_sex_by_grade[sex] = d

    cur.execute(query_sex_enrollment_counts())
    for grade, sex, total in cur.fetchall():
        grade = convert_nces_grade(grade)
        by_grade = enrollment_by_sex_by_grade[sex]
        by_grade[grade] = total
        by_grade['total'] = by_grade.get('total', 0) + total
        enrollment_by_sex_by_grade[sex] = by_grade

    return enrollment_by_sex_by_grade

def get_enrollment_by_grade_by_race(cur, year, school):
    nces_id = school['nces_id']
    low_grade = school['low_grade']
    high_grade = school['high_grade']

    check_where = andd([
        idequals('year', year),
        idequals('nces_id', nces_id)
    ])
    query = select('nces_enrollment_counts', '*', where=check_where)
    cur.execute(query)
    if not cur.fetchone():
        return None

    def query_race_enrollment_counts():
        table = sql.Identifier('nces_enrollment_counts')

        where = andd([
            idequals('year', year),
            idequals('nces_id', nces_id)
        ])

        def sumcol(race):
            return sql.SQL("""sum({0}::integer) as {0}""").format(sql.Identifier(race))

        race_cols = sql.SQL(', ').join(sumcol(col) for col in RACE_COLS)

        query = sql.SQL(
            """select grade, {} from {} where {} group by grade"""
        ).format(race_cols, table, where)

        return query

    enrollment_by_grade_by_race = {}
    grades = get_grades(low_grade, high_grade)
    for grade in grades:
        for race in RACE_COLS:
            race_dict = {'count': 0, 'percent': 0.0}
            by_race = enrollment_by_grade_by_race.get(grade, {})
            by_race[race] = race_dict
            enrollment_by_grade_by_race[grade] = by_race

    cur.execute(query_race_enrollment_counts())
    for row in cur.fetchall():
        grade = convert_nces_grade(row[0])
        assert grade in grades, f'Grade {grade} not in valid grades for school ({grades})'
        races = row[1:]
        assert len(races) == len(RACE_COLS)
        if races[-1] == 0: # total
            assert all(count == 0 for count in races), races
            continue

        race_percent = [ count / float(races[-1]) * 100 for count in races ]
        race_dict = {}
        for race, count, percent in zip(RACE_COLS, races, race_percent):
            race_dict[race] = { 'count': count, 'percent': percent }

        enrollment_by_grade_by_race[grade] = race_dict

    return enrollment_by_grade_by_race

def get_staff_by_sex_by_category(cur, year, state_lea_id, state_school_id):
    check_where = andd([
        idequals('year', year),
        idequals('state_lea_id', state_lea_id),
        idequals('state_school_id', state_school_id)
    ])

    query = select('appointments', '*', where=check_where)
    cur.execute(query)
    if not cur.fetchone():
        return None

    table = sql.Identifier('appointments')
    where = andd([
        idequals('year', year),
        idequals('state_lea_id', state_lea_id),
        idequals('state_school_id', state_school_id),
    ])

    query = sql.SQL(
        """select COALESCE(position_category, 'Other') as category, gender, count(gender) from {} where {} group by category, gender"""
    ).format(table, where)

    cur.execute(query)

    staff_by_sex_by_category = {}
    for category, gender, count in cur.fetchall():
        by_category = staff_by_sex_by_category.get(gender, {})
        by_category[category] = count
        staff_by_sex_by_category[gender] = by_category

    for sex in ('M', 'F'):
        for category in STAFF_CATEGORIES:
            by_category = staff_by_sex_by_category.get(sex, {})
            if category not in by_category:
                by_category[category] = 0
            by_category['Total'] = by_category.get('Total', 0) + by_category[category]
            staff_by_sex_by_category[sex] = by_category
    return staff_by_sex_by_category

def get_staff_by_category_by_education(cur, year, state_lea_id, state_school_id):
    check_where = andd([
        idequals('year', year),
        idequals('state_lea_id', state_lea_id),
        idequals('state_school_id', state_school_id)
    ])

    query = select('appointments', '*', where=check_where)
    cur.execute(query)
    if not cur.fetchone():
        return None

    table = sql.Identifier('appointments')

    where = andd([
        idequals('year', year),
        idequals('state_lea_id', state_lea_id),
        idequals('state_school_id', state_school_id),
    ])
    query = sql.SQL(
        """select position_category, education_level, COALESCE(sum(fte), 0) from {} where {} and position_category is not null group by position_category, education_level"""
    ).format(table, where)

    cur.execute(query)
    staff_by_category_by_education = {}
    for category, education_level, count in cur.fetchall():
        by_education = staff_by_category_by_education.get(category, {})
        by_education[EDUCATION_MAP[education_level]] = count
        staff_by_category_by_education[category] = by_education


    for category in STAFF_CATEGORIES:
        for education in STAFF_EDUCATION_LEVELS:
            by_education = staff_by_category_by_education.get(category, {})
            if education not in by_education:
                by_education[education] = 0
            by_education['Total'] = by_education.get('Total', 0) + by_education[education]
            staff_by_category_by_education[category] = by_education


    return staff_by_category_by_education


def format_rec(sq, *format):
    if hasattr(sq, '__iter__'):
        return sql.SQL('').join(format_rec(c, *format) for c in sq)
    else:
        return sq.format(*format)


def get_staff_by_category_by_tenure(cur, state_lea_id, state_school_id):
    def query_staff_tenure_counts(year_constraints):
        table = sql.Identifier('appointments')

        where1 = andd([
            idequals('state_lea_id', state_lea_id),
            idequals('state_school_id', state_school_id),
        ])
        where2 = format_rec(year_constraints, sql.Identifier('s3', 'count_year'))

        query = sql.SQL(
            """select position_category, count(count_year) from (select first_name, last_name, position_category, count(year) as count_year from (select * from (select first_name, last_name, position_category, year, sum(fte) as sum_fte from {} where {} group by first_name, last_name, position_category, year) as s1 where s1.sum_fte > 0.0) as s2  group by first_name, last_name, position_category order by first_name,last_name) as s3 where {} group by position_category"""
        ).format(table, where1, where2)

        return query

    q_1_2 = query_staff_tenure_counts(
        orr([sql.SQL("""{0} = 1"""), sql.SQL("""{0} = 2""")])
    )
    q_3_4 = query_staff_tenure_counts(
        orr([sql.SQL("""{0} = 3"""), sql.SQL("""{0} = 4""")])
    )
    q_5_plus = query_staff_tenure_counts(
        sql.SQL("""{0} >= 5""")
    )

    tenure_items = (
        (q_1_2, '1_2'),
        (q_3_4, '3_4'),
        (q_5_plus, '5_plus'),
    )

    tenure_keys = [ item[1] for item in tenure_items ]

    staff_by_category_by_tenure = {}
    for query, key in tenure_items:
        cur.execute(query)
        for category, count in cur.fetchall():
            by_tenure = staff_by_category_by_tenure.get(category, {})
            by_tenure[key] = count
            staff_by_category_by_tenure[category] = by_tenure

    for tenure in tenure_keys:
        for category in STAFF_CATEGORIES:
            by_tenure = staff_by_category_by_tenure.get(category, {})
            if tenure not in by_tenure:
                by_tenure[tenure] = 0
            by_tenure['Total'] = by_tenure.get('Total', 0) + by_tenure[tenure]
            staff_by_category_by_tenure[category] = by_tenure

    return staff_by_category_by_tenure

iss_categories = [
    'SWD: Students receiving one or more in-school suspensions',
    'SWOD: Students receiving one or more in-school suspensions'
]

oss_categories = [
    'SWD: Students receiving more than one out-of-school suspension',
    'SWD: Students receiving only one out-of-school suspension',
    'SWOD: Students receiving more than one out-of-school suspension',
    'SWOD: Students receiving only one out-of-school suspension',
]

referral_categories = [
    'SWD: Referral to law enforcement',
    'SWOD: Referral to law enforcement',
]

def get_discipline_by_type_by_sex(cur, year, nces_id):
    check_where = andd([
        idequals('nces_id', nces_id),
        idequals('year', year)
    ])
    query = select('discipline_counts', '*', where=check_where)
    cur.execute(query)
    if not cur.fetchone():
        return None

    def query_sex_counts(categories):

        table = sql.Identifier('discipline_counts')
        orr_categories = orr(idequals('category', category) for category in categories)

        statement = sql.SQL("""select sex, sum(total::integer) as count from {} where {} group by sex""").format(
            table,
            andd([
                idequals('nces_id', nces_id),
                idequals('year', year),
                orr_categories]))
        return statement

    cur.execute(query_sex_counts(iss_categories))
    iss_counts = dict(cur.fetchall())

    cur.execute(query_sex_counts(oss_categories))
    oss_counts = dict(cur.fetchall())
    #print(oss_counts)

    cur.execute(query_sex_counts(referral_categories))
    referral_counts = dict(cur.fetchall())

    by_type_by_sex = {}
    by_type_by_sex['iss'] = iss_counts
    by_type_by_sex['oss'] = oss_counts
    by_type_by_sex['referral'] = referral_counts

    return by_type_by_sex

def get_discipline_by_type_by_race(cur, year, nces_id):
    check_where = andd([
        idequals('nces_id', nces_id),
        idequals('year', year)
    ])
    query = select('discipline_counts', '*', where=check_where)
    cur.execute(query)
    if not cur.fetchone():
        return None

    def query_race_counts(categories):

        table = sql.Identifier('discipline_counts')
        orr_categories = orr(idequals('category', category) for category in categories)

        def sum_col(identifier):
            return sql.SQL("""round(sum({0}::float))::int as {0}""").format(sql.Identifier(identifier))

        cols = sql.SQL(', ').join(sum_col(race_col) for race_col in RACE_COLS)

        statement = sql.SQL("""select {} from {} where {}""").format(
            cols,
            table,
            andd([
                idequals('nces_id', nces_id),
                idequals('year', year),
                orr_categories]))
        return statement

    def make_races_dict(cols):
        return dict(zip(RACE_COLS, cols))

    def get_race_counts(cur, categories):
        statement = query_race_counts(categories)
        cur.execute(statement)
        cols = cur.fetchall()
        assert len(cols) == 1, 'bad len'
        counts = make_races_dict(cols[0])
        return counts

    by_type_by_race = {
        'iss': get_race_counts(cur, iss_categories),
        'oss': get_race_counts(cur, oss_categories),
        'referral': get_race_counts(cur, referral_categories),
    }

    return by_type_by_race

