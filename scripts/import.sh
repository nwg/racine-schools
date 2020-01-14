#!/bin/sh

set -e

export PYTHONPATH=..:../util

./import_manual_data.py
./import_assignments_oldstyle.py 2014 data/downloaded/all-staff-2014-2015.csv
./import_assignments_oldstyle.py 2015 data/downloaded/all-staff-2015-2016.csv
./import_assignments_newstyle.py 2016 data/downloaded/all-staff-2016-2017.csv
./import_assignments_newstyle.py 2017 data/downloaded/all-staff-2017-2018.csv
./import_assignments_newstyle.py 2018 data/downloaded/all-staff-2018-2019.csv
./import_discipline.py 2015 data/discipline_rusd_2015.csv
