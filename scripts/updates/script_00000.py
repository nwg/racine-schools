
from psycopg2 import sql

def execute(cur):
    q = sql.SQL('select nces_id from schools where longname={}') \
            .format(sql.Literal('Racine Alternative Learning'))
    cur.execute(q)
    row = cur.fetchone()
    nces_id = row['nces_id']

    q = sql.SQL('delete from discipline_counts where nces_id={}').format(sql.Literal(nces_id))
    cur.execute(q)
