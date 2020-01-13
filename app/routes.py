from app import app
from app.render import render_school_with_name

@app.route('/s/<schoolname>')
def school(schoolname):
    return render_school_with_name(schoolname)

@app.route('/')
@app.route('/index')
def index():
    return "Hello, World!"
