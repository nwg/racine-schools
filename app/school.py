from flask import render_template
from app import app
from app import db
from app.db import make_cursor
from psycopg_utils import idequals, andd, orr

def render_schools_index():
    cur = make_cursor()
    schools = db.all_schools(cur)
    return render_template(
        'index.html',
        schools=schools
    )
