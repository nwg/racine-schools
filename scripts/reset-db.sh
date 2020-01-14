#!/bin/sh

set -e

dropdb -U postgres schools
createdb -U postgres schools
psql -U postgres schools <create.sql
