import psycopg2
import psycopg2.extras
from psycopg_utils import select, idequals
from psycopg2 import sql

conn = psycopg2.connect("dbname='schools' user='postgres' host='localhost' password=''")
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

def school_with_name(name):
    sql = select('schools', '*', where=idequals('longname', name))
    cur.execute(sql)
    return cur.fetchone()

def all_schools():
    q = sql.SQL("""select * from schools order by longname""")
    cur.execute(q)
    return cur.fetchall()
