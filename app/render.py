from flask import render_template
from app import app
from app import db
from app.db import conn, cur
from psycopg_utils import idequals, andd, orr
from psycopg2 import sql

MOST_RECENT_STAFF_YEAR = 2018

EDUCATION_MAP = {
    3: 'associate',
    4: 'bachelors',
    5: 'masters',
    6: 'six_year_specialists',
    7: 'doctorate',
    8: 'other',
    None: 'unreported',
}

def render_school_with_name(name):
    school = db.school_with_name(name)
    state_lea_id = school['state_lea_id']
    state_school_id = school['state_school_id']
    #assert False, school
    return render_template(
        'school.html',
        school=school,
        staff_by_category_by_sex=get_staff_by_category_by_sex(cur, MOST_RECENT_STAFF_YEAR, state_lea_id, state_school_id),
        staff_by_category_by_education=get_staff_by_category_by_education(cur, state_lea_id, state_school_id),
        staff_by_category_by_tenure=get_staff_by_category_by_tenure(cur, state_lea_id, state_school_id),
    )

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

    return staff_by_category_by_tenure
