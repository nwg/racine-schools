
from psycopg2 import sql

def execute(cur):
    q = sql.SQL('''
        create table hide_data_ui (
            school_id integer references schools (id),
            hide_where text not null
                check (hide_where in
                    ('staff_by_category_by_tenure')
                ),
            hidden boolean not null
        )
    ''')
    cur.execute(q)

    q = sql.SQL('select id from schools where longname={}') \
            .format(sql.Literal('Racine Alternative Learning'))
    cur.execute(q)
    row = cur.fetchone()
    school_id = row['id']

    q = sql.SQL('insert into hide_data_ui values ({}, {}, {})') \
        .format(
            sql.Literal(school_id),
            sql.Literal('staff_by_category_by_tenure'),
            sql.Literal('true')
        )
    cur.execute(q)
