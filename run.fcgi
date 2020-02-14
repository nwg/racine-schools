#!/bin/sh

export PYTHONPATH=./util
cd $(dirname "$0")
exec pipenv run python3.7 ./racine-schools-fcgi.py
