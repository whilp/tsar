#!/bin/sh

BULK_CONTENT_TYPE=text/csv
CURL="curl -s"
OBSERVATION_ENDPOINT=observations
SERVER=http://tsar.hep.wisc.edu

# Record a new observation. 'time' is an integer representing seconds
# since the Unix Epoch, UTC (ie `date '+%s'`). 'subject' is a string
# indicating the host or service being monitored (ie "dcache_pfm",
# "condor_users_${USERNAME}", "g9n01.hep.wisc.edu"). 'attribute' is the
# property of the subject being monitored ("runtime_seconds", "held_jobs",
# "free_virtual_memory_kb"). 'cf' is a string indicating the consolidation
# function the server should use when aggregating data for this series.
#
# The 'subject' and 'attribute' fields are free-form. By convention,
# fields with multiple elements in a hierarchy (ie, "dcache" and
# "pfm") are delimited by underscores. 'cf' must match a function implemented
# by the server
#
# The following request would record a new observation for the 'bar'
# attribute of the 'foo' subject using the 'max' consolidation function:
#
#    tsar_record foo bar max $(date '+%s') 10
#
tsar_record () {
    ${CURL} \
        -d "subject=$1" \
        -d "attribute=$2" \
        -d "attribute=$3" \
        -d "time=$4" \
        -d "value=$5" \
        "${SERVICE}"
}

# tsar_record "condor_users_${USER_NAME}" held_jobs last "${DATE}" "${HELD_JOBS}"
