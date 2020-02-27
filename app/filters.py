from app import app

@app.template_filter()
def grade_format(s):
    if s.isdigit():
        return str(int(s))
    return s
