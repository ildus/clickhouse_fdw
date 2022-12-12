#!/bin/bash

# This is a main testing script for:
#	* regression tests
#	* testgres-based tests
#	* cmocka-based tests
# Copyright (c) 2017, Postgres Professional

set -eux

echo CHECK_CODE=$CHECK_CODE

status=0

# run cmake
mkdir -p build
pushd build
cmake -DCMAKE_BUILD_TYPE=Debug ..

# perform code analysis if necessary
if [ "$CHECK_CODE" = "clang" ]; then
    scan-build --status-bugs make || status=$?
    exit $status
fi

# initialize database
initdb

# build and install extension
make clean
make -j10
make install

# check build
status=$?
if [ $status -ne 0 ]; then exit $status; fi

# add the extension to shared_preload_libraries and restart cluster 'test'
echo "port = 55435" >> $PGDATA/postgresql.conf
pg_ctl start -l /tmp/postgres.log -w

# check startup
status=$?
if [ $status -ne 0 ]; then cat /tmp/postgres.log; fi

# run regression tests
export PG_REGRESS_DIFF_OPTS="-w -U3" # for alpine's diff (BusyBox)
PGPORT=55435 make installcheck || status=$?

# show diff if it exists
popd
if test -f tests/regression.diffs; then cat tests/regression.diffs; fi

# check startup
if [ $status -ne 0 ]; then cat /tmp/postgres.log; fi
exit $status
