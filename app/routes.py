from app import app
from app.school_summary import render_school_summary_with_name
from app.school import render_schools_index

@app.route('/s/<schoolname>')
def school(schoolname):
    return render_school_summary_with_name(schoolname)

@app.route('/')
@app.route('/index')
def index():
    return render_schools_index()
