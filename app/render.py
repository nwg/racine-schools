from flask import render_template
from app import app
from app import db

def render_school_with_name(name):
    school = db.school_with_name(name)
    #assert False, school
    return render_template('school.html', school=school)
