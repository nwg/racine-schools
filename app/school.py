from flask import render_template
from app import app
from app import db
from app.db import conn, cur
from psycopg_utils import idequals, andd, orr

def render_schools_index():
    schools = db.all_schools()
    return render_template(
        'index.html',
        schools=schools
    )
