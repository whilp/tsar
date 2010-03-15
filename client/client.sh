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
# "free_virtual_memory_kb").
#
# The 'subject' and 'attribute' fields are free-form. By convention,
# fields with multiple elements in a hierarchy (ie, "dcache" and
# "pfm") are delimited by underscores.
#
# The following request would record a new observation for the 'bar'
# attribute of the 'foo' subject:
#
#    tsar_record foo bar $(date '+%s') 10
#
tsar_record () {
    ${CURL} \
        -d "subject=$1" \
        -d "attribute=$2" \
        -d "time=$3" \
        -d "value=$4" \
        "${SERVER}/${OBSERVATION_ENDPOINT}"
}

# tsar also supports bulk submission of observations. Bulk records
# should be formatted in a text file as comma-separated values with column
# headers. Field values are as defined in tsar_record(). For example:
#     
#     time,subject,attribute,value
#     1268665017,foo,bar,10
#     1268665027,foo,bar,12
#     1268665037,foo,bar,11
#     1268665047,foo,bar,10
#     [...]
#     
# Bulk submissions may include records for more than one (subject,
# attribute) pair. The CSV text should be sent on standard input.
# For example:
# 
#    tsar_bulk < /path/to/observations.csv
#
tsar_bulk () {
    ${CURL} -F \
        "${OBSERVATION_ENDPOINT}=@-;type=${BULK_CONTENT_TYPE}" \
        "${SERVER}/${OBSERVATION_ENDPOINT}"
}

# Clients may request a range of records for a given (subject,
# attribute) pair. 'subject' and 'attribute' are strings as defined in
# tsar_record(). 'start' and 'stop' are either valid 'time' values as
# defined in tsar_record() or negative integers representing points in
# time counted back from now. For example, the following request would
# return all records for (foo, bar):
#    tsar_query foo bar 0 -1
#
tsar_query () {
    ${CURL} -G \
        -d "subject=$1" \
        -d "attribute=$2" \
        -d "start=$3" \
        -d "stop=$4" \
        "${SERVER}/${OBSERVATION_ENDPOINT}"
}
