#!/bin/sh

export FLASK_APP=schools.py
export FLASK_ENV=development
export PYTHONPATH=./util
pipenv run flask run --host=0.0.0.0
