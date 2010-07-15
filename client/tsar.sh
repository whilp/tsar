#!/bin/sh

TSAR_SERVICE=http://tsar.hep.wisc.edu/records
TSAR_MEDIA=application/vnd.tsar.records.v2
TSAR_AGENT=
TSAR_CURL="curl -s -A '${TSAR_AGENT}'"
TSAR_HEADERS="subject,attribute,cf,timestamp,value"

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
# underscores. 'subject', 'attribute' and 'cf' should be URL-encoded (escaping
# reserved characters per RFC 2396).
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

    echo "${SUBJECT},${ATTRIBUTE},${CF},${TIMESTAMP},${VALUE}" | tsar_bulk 
}

# tsar also supports bulk submission of observations. Each bulk record must have
# all fields as defined in tsar_record(), formatted as a series of
# comma-separated values (CSV)
#
#     foo,bar,last,1268665017,10
#     foo,bar,last,1268665027,10
#     foo,bar,last,1268665037,10
#     foo,bar,last,1268665047,10
#     [...]
#
# For example (assuming observations.csv contains the above text):
# 
#    tsar_bulk < /path/to/observations.csv
#
tsar_bulk () {
    local LINE
    (echo ${TSAR_HEADERS}; \
        while read LINE; do  \
            case $LINE in ${TSAR_HEADERS}) continue; esac
            echo $LINE
        done) |\
        ${TSAR_CURL} --data-binary @- -H "Content-Type: ${TSAR_MEDIA}+csv" \
            "${TSAR_SERVICE}"
}

# Clients may request a range of records for a given (subject, attribute) pair.
# 'subject' and 'attribute' are strings as defined in tsar_record(). 'start'
# and 'stop' are either valid 'time' values as defined in tsar_record() or
# negative integers representing points in time counted back from now. 'cf' is
# an optional string as defined in tsar_record(). For example, the following
# request would return all records for (foo, bar) with the default consolidation
# function:
#
#    tsar_query foo bar 0 -1
#
# Successful queries output one or more lines of records, formatted as CSV.
# The output of the above query might look like:
#
#    foo,bar,last,1268665030,10
#    foo,bar,last,1268665040,10
#    foo,bar,last,1268665050,10
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

    local LINE
    local RESOURCE=${SUBJECT}/${ATTRIBUTE}/${CF}

    local CMD="${TSAR_CURL} -G -d start=${START} -d stop=${STOP}"
    if [ -n "${NOW}" ]; then
        CMD="${CMD} -d now=${NOW}"
    fi

    ${CMD} "${TSAR_SERVICE}/${RESOURCE}" |\
        while read LINE; do
            case $LINE in ${TSAR_HEADERS}*) continue;; esac
            echo ${LINE}
        done
}
