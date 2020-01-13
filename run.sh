#!/bin/sh

export FLASK_APP=schools.py
pipenv run flask run --host=0.0.0.0
