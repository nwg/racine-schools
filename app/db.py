import psycopg2
import psycopg2.extras
from psycopg_utils import select, idequals

conn = psycopg2.connect("dbname='schools' user='postgres' host='localhost' password=''")
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

def school_with_name(name):
    sql = select('schools', '*', where=idequals('longname', name))
    cur.execute(sql)
    return cur.fetchone()
