#!/bin/sh

# chkconfig: 2345 99 01
# description: tsar
# processname: tsar-server

HOST=0.0.0.0:8080
DIR=/scratch/tsar
BINDIR=${DIR}/bin
PIDFILE=${DIR}/tsar.pid
LOGFILE=${DIR}/tsar.log
USER=tsar:tsar
prog=tsar-server

. /etc/rc.d/init.d/functions

start ()
{
    echo -n "Starting $prog:"
    # XXX: --check?
    daemon --pidfile="${PIDFILE}" --user="${USER%:*}" ${BINDIR}/tsar-server \
        -dv \
        -l "${LOGFILE}" \
        -p "${PIDFILE}" \
        -D redis://localhost:6379/0 \
        ${HOST} >/dev/null 2>&1
    RETVAL=$?
    echo
    return ${RETVAL}
}

stop ()
{
    if [ ! -r "${PIDFILE}" ]; then
        echo "$prog is not running"
        return 1
    fi

    echo -n "Shutting down $prog: "
    killproc -p "${PIDFILE}" $prog
    RETVAL=$?
    echo
    return ${RETVAL}
}

case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    *)
        echo "Usage: $0 {start|stop}"
        ;;
esac
