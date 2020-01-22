from flask import render_template
from app import app
from app import db
from app.db import conn, cur
from psycopg_utils import select, idequals, andd, orr, on, colsequal
from psycopg2 import sql

MOST_RECENT_STAFF_YEAR = 2018

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

GRADES_ORDER = ('PK', 'K3', 'K4', 'K5', 'KG', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13')

def render_school_summary_with_name(name):
    school = db.school_with_name(name)
    state_lea_id = school['state_lea_id']
    state_school_id = school['state_school_id']
    nces_id = school['nces_id']
    ppin = school['pss_ppin']

    d = {}
    if state_lea_id and state_school_id:
        d['staff_by_category_by_sex'] = get_staff_by_category_by_sex(cur, MOST_RECENT_STAFF_YEAR, state_lea_id, state_school_id)
        d['staff_by_category_by_education'] = get_staff_by_category_by_education(cur, state_lea_id, state_school_id)
        d['staff_by_category_by_tenure'] = get_staff_by_category_by_tenure(cur, state_lea_id, state_school_id)

    if nces_id:
        d['discipline_by_type_by_sex'] = get_discipline_by_type_by_sex(cur, nces_id)
        d['discipline_by_type_by_race'] = get_discipline_by_type_by_race(cur, nces_id)
        d['enrollment_grade_sex_data'] = get_enrollment_grade_sex_data(cur, nces_id)
        d['enrollment_by_grade_by_race'] = get_enrollment_by_grade_by_race(cur, nces_id)

    if ppin:
        d['pss_info'] = get_pss_info(cur, ppin)
        d['pss_enrollment_by_grade'] = get_pss_enrollment_by_grade(cur, ppin)
        d['pss_enrollment_by_demographic'] = get_pss_enrollment_by_demographic(cur, ppin)

    return render_template(
        'school.html',
        school=school,
        **d
    )

def get_pss_info(cur, ppin):
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
        return select('pss_info', '*', where=idequals('ppin', ppin), join=joins)
    q = query_pss_info()
    cur.execute(q)

    return cur.fetchone()

def get_pss_enrollment_by_grade(cur, ppin):
    def query_pss_enrollment():
        where = andd([idequals('year', 2017), idequals('ppin', ppin)])
        return select('pss_enrollment_grade_counts', (sql.Identifier('grade'), sql.Identifier('enrollment')), where=where)
    q = query_pss_enrollment()
    cur.execute(q)
    enrollment_by_grade = {}
    for row in cur.fetchall():
        enrollment_by_grade[row['grade']] = row['enrollment']

    return enrollment_by_grade

def get_pss_enrollment_by_demographic(cur, ppin):
    def query_pss_enrollment():
        where = andd([idequals('year', 2017), idequals('ppin', ppin)])
        return select('pss_enrollment_demographic_counts', '*', where=where)
    q = query_pss_enrollment()
    cur.execute(q)

    return cur.fetchone()


def get_enrollment_grade_sex_data(cur, nces_id):
    def query_sex_enrollment_counts():
        table = sql.Identifier('nces_enrollment_counts')

        where = andd([
            idequals('year', 2017),
            idequals('nces_id', nces_id)
        ])

        query = sql.SQL(
            """select grade, sex, sum(total::integer) as total from {} where {} group by grade, sex"""
        ).format(table, where)

        return query

    cur.execute(query_sex_enrollment_counts())

    grades = set()
    enrollment_by_sex_by_grade = {}
    for grade, sex, total in cur.fetchall():
        grades.add(grade)
        by_grade = enrollment_by_sex_by_grade.get(sex, {})
        by_grade[grade] = total
        by_grade['total'] = by_grade.get('total', 0) + total
        enrollment_by_sex_by_grade[sex] = by_grade

    return {
        'enrollment_by_sex_by_grade': enrollment_by_sex_by_grade,
        'grades': sorted(grades, key=lambda x: GRADES_ORDER.index(x))
    }

def get_enrollment_by_grade_by_race(cur, nces_id):
    def query_race_enrollment_counts():
        table = sql.Identifier('nces_enrollment_counts')

        where = andd([
            idequals('year', 2017),
            idequals('nces_id', nces_id)
        ])

        def sumcol(race):
            return sql.SQL("""sum({0}::integer) as {0}""").format(sql.Identifier(race))

        race_cols = sql.SQL(', ').join(sumcol(col) for col in RACE_COLS)

        query = sql.SQL(
            """select grade, {} from {} where {} group by grade"""
        ).format(race_cols, table, where)

        return query

    cur.execute(query_race_enrollment_counts())
    enrollment_by_grade_by_race = {}
    for row in cur.fetchall():
        grade = row[0]
        races = row[1:]
        if races[-1] > 0:
            race_percent = [ count / float(races[-1]) * 100 for count in races ]
        else:
            assert all(count == 0 for count in races), races
            race_percent = [ 0.0 ] * len(races)
        assert len(races) == len(RACE_COLS)
        race_dict = dict(zip(RACE_COLS, zip(races, race_percent)))
        enrollment_by_grade_by_race[grade] = race_dict

    return enrollment_by_grade_by_race

def get_staff_by_category_by_sex(cur, year, state_lea_id, state_school_id):
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

    staff_by_category_by_sex = {}
    for category, gender, count in cur.fetchall():
        by_sex = staff_by_category_by_sex.get(category, {})
        by_sex[gender] = count
        staff_by_category_by_sex[category] = by_sex

    if not staff_by_category_by_sex:
        return None

    return staff_by_category_by_sex

def get_staff_by_category_by_education(cur, state_lea_id, state_school_id):
    table = sql.Identifier('appointments')

    where = andd([
        idequals('year', 2018),
        idequals('state_lea_id', state_lea_id),
        idequals('state_school_id', state_school_id),
    ])
    query = sql.SQL(
        """select position_category, education_level, COALESCE(sum(fte), 0) from {} where {} group by position_category, education_level"""
    ).format(table, where)

    cur.execute(query)
    staff_by_category_by_education = {}
    for category, education_level, count in cur.fetchall():
        by_education = staff_by_category_by_education.get(category, {})
        by_education[EDUCATION_MAP[education_level]] = count
        staff_by_category_by_education[category] = by_education

    if not staff_by_category_by_education:
        return None

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

    staff_by_category_by_tenure = {}
    for query, key in tenure_items:
        cur.execute(query)
        for category, count in cur.fetchall():
            by_tenure = staff_by_category_by_tenure.get(category, {})
            by_tenure[key] = count
            staff_by_category_by_tenure[category] = by_tenure

    if not staff_by_category_by_tenure:
        return None

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

def get_discipline_by_type_by_sex(cur, nces_id):
    query = select('discipline_counts', '*', where=idequals('nces_id', nces_id))
    cur.execute(query)
    if not cur.fetchone():
        return None

    def query_sex_counts(categories):

        table = sql.Identifier('discipline_counts')
        orr_categories = orr(idequals('category', category) for category in categories)

        statement = sql.SQL("""select sex, sum(total::integer) as total from {} where {} group by sex""").format(
            table,
            andd([
                idequals('nces_id', nces_id),
                idequals('year', 2015),
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

def get_discipline_by_type_by_race(cur, nces_id):
    query = select('discipline_counts', '*', where=idequals('nces_id', nces_id))
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

