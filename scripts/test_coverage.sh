#!/bin/bash
# The shell is for view test coverage.

COVERAGE_FILE=test_coverage.info
REPORT_FOLDER=test_coverage_report
./../build/dbtest
lcov --rc lcov_branch_coverage=1 -c -d $(pwd)/../build/ -o ${COVERAGE_FILE}_tmp
lcov --rc lcov_branch_coverage=1 -e ${COVERAGE_FILE}_tmp "*engine*" -o ${COVERAGE_FILE}
genhtml --rc genhtml_branch_coverage=1 ${COVERAGE_FILE} -o ${REPORT_FOLDER}
