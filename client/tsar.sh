#!/bin/sh

TSAR_SERVICE=http://tsar.hep.wisc.edu/records/
TSAR_MEDIA=application/vnd.tsar.records.v1
TSAR_AGENT=
TSAR_CURL="curl -s -A '${TSAR_AGENT}'"

# Record a new observation. 'timestamp' is an integer representing seconds since
# the Unix Epoch, UTC (ie `date '+%s'`). 'subject' is a string indicating the
# host or service being monitored (ie "dcache_pfm", "condor_users_${USERNAME}",
# "g9n01.hep.wisc.edu"). 'attribute' is the property of the subject being
# monitored ("runtime_seconds", "held_jobs", "free_virtual_memory_kb"). 'last'
# is an optional argument and should be a string indicating the name of the
# function the server should use to consolidate the data ("last", "min", "max",
# ...).
#
# The 'subject' and 'attribute' fields are free-form. By convention, fields with
# multiple elements in a hierarchy (ie, "dcache" and "pfm") are delimited by
# underscores.
#
# The following request would record a new observation for the 'bar' attribute
# of the 'foo' subject:
#
#    tsar_record foo bar $(date '+%s') 10
#
tsar_record () {
    local SUBJECT=$1
    local ATTRIBUTE=$2
    local TIMESTAMP=$3
    local VALUE=$4
    local CF=${5:-last}

    echo "${TIMESTAMP},${VALUE}" |\
        tsar_bulk "${SUBJECT}" "${ATTRIBUTE}" "${CF}"
}

# tsar also supports bulk submission of observations. Bulk records should be
# formatted as comma-separated values (CSV) with column headers and passed to
# tsar_bulk on stdin. Two columns, 'timestamp' and 'value', are defined as in
# tsar_record:
#
#     timestamp,value
#     1268665017,10
#     1268665027,12
#     1268665037,11
#     1268665047,10
#     [...]
#
# As with tsar_record, the records' ssubject and attribute should be passed as
# command line arguments. If the desired consolidation function is omitted, a
# default value will be used.
#
# For example (assuming observations.csv contains the above text):
# 
#    tsar_bulk foo bar < /path/to/observations.csv
#
tsar_bulk () {
    local SUBJECT=$1
    local ATTRIBUTE=$2
    local CF=${3:-last}

    local RESOURCE=${SUBJECT}/${ATTRIBUTE}/${CF}
    local DATA="timestamp,value\n"
    while read LINE; do

    (echo "timestamp,value"; while read LINE; do echo $LINE; done) |\
    #${TSAR_CURL} --request POST --data-binary  --header "Content-Type: ${TSAR_MEDIA}+csv"
    ${TSAR_CURL} -F \
        "@-;type=${TSAR_MEDIA}+csv" "${TSAR_SERVICE}/${RESOURCE}"
}

# Clients may request a range of records for a given (subject, attribute) pair.
# 'subject' and 'attribute' are strings as defined in tsar_record(). 'start'
# and 'stop' are either valid 'time' values as defined in tsar_record() or
# negative integers representing points in time counted back from now. 'cf' is
# an optional string as defined in tsar_record(). For example, the following
# request would return all records for (foo, bar):
#
#    tsar_query foo bar 0 -1
#
# Successful queries output one or more lines of records, with the 'timestamp'
# and 'value' fields separated by a space. The output of the above query might
# look like:
#
#    1268665030 10
#    1268665040 12
#    1268665050 14
#
# Additional options:
#    cf         (string)    Condolidation function.
#    now        (integer)   Use 'now' when computing relative times.
#
tsar_query () {
    local SUBJECT=$1
    local ATTRIBUTE=$2
    local START=$3
    local STOP=$4
    local CF=${5:-"last"}
    local NOW=$6

    local RESOURCE=${SUBJECT}/${ATTRIBUTE}/${CF}

    local CMD="${CURL} -G -d start=${START} -d stop=${STOP}"
    if [ -n "${NOW}" ]; then
        CMD="${CMD} -d now=${NOW}"
    fi

    ${CMD} "${TSAR_SERVICE}/${RESOURCE}" | while read LINE; do
        TIMESTAMP=${LINE%,*}
        VALUE=${LINE#*}
        echo ${TIMESTAMP} ${VALUE}
    done
}
