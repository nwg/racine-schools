#!/usr/bin/env python3

import psycopg2
from psycopg2 import sql
import argparse
import sys

description = """\
Finalize the appointments data by creating helper tables
"""

parser = argparse.ArgumentParser(description=description)
parser.add_argument('year', type=int, help='The lower year of the school year for the directory being imported')
args = parser.parse_args()


if __name__ == '__main__':

    conn = psycopg2.connect("dbname='schools' user='postgres' host='localhost' password=''")
    cur = conn.cursor()

    lit_year = sql.Literal(args.year)

    q = sql.SQL("""select * from appointments_distinct_ranked_most_recent where year={} limit 1""").format(lit_year)
    cur.execute(q)
    if cur.fetchone():
        print(f'Already imported appointments for year {args.year}')
        sys.exit(0)
  
    q = sql.SQL("""delete from appointments_distinct_ranked_most_recent""")
    cur.execute(q)

    q = sql.SQL("""
    insert into appointments_distinct_ranked_most_recent (year, state_lea_id, state_school_id, first_name, last_name, race, position_category, gender, education_level)
    select distinct a2.year, a2.state_lea_id, a2.state_school_id, a2.first_name, a2.last_name, a2.race, a2.position_category, a2.gender, a2.education_level from (
        select distinct s1.first_name, s1.last_name, s1.state_lea_id, s1.state_school_id, r2.position_category from (
            select a.first_name, a.last_name, a.state_lea_id, a.state_school_id, MAX(r.rank) as rank from appointments a
            inner join position_category_rank r
            using (position_category)
            where a.year={}
            group by a.first_name, a.last_name, a.state_lea_id, a.state_school_id
        ) s1
        join position_category_rank r2 using (rank)
    ) s2
    join appointments a2 using (first_name, last_name, state_lea_id, state_school_id, position_category)
    where a2.year={};
    """).format(lit_year, lit_year)
    cur.execute(q)

    conn.commit()
