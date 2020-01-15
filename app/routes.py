from app import app
from app.school_summary import render_school_summary_with_name

@app.route('/s/<schoolname>')
def school(schoolname):
    return render_school_summary_with_name(schoolname)

@app.route('/')
@app.route('/index')
def index():
    return "Hello, World!"
