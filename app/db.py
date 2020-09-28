import psycopg2
import psycopg2.extras
from psycopg_utils import select, idequals
from psycopg2 import sql


def make_conn():
    return psycopg2.connect("dbname='schools' user='postgres' host='localhost' password=''")

conn = make_conn()

def make_cursor():
    global conn
    try:
        return conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    except psycopg2.InterfaceError:
        conn = make_conn()

def school_with_name(cur, name):
    q = select('schools', '*', where=idequals('longname', name))
    cur.execute(q)
    return cur.fetchone()

def all_schools(cur):
    q = sql.SQL("""select * from schools order by longname""")
    cur.execute(q)
    return cur.fetchall()
