#!/bin/sh

dropdb -U postgres schools
createdb -U postgres schools
psql -U postgres schools <create.sql
