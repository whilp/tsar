#!/bin/sh

# chkconfig: 2345 99 01
# description: tsar
# processname: tsar-server

ROOT=/scratch/tsar
PIDFILE=${ROOT}/logs/supervisord.pid
PROG=tsar

. /etc/rc.d/init.d/functions

start () {
    echo -n "Starting ${PROG}:"
	cd ${ROOT}
	${ROOT}/bin/supervisord \
		-c ${ROOT}/scripts/supervisor.conf \
		-u tsar \
		-j "${PIDFILE}"
    RETVAL=$?
    echo
    return ${RETVAL}
}

stop ()
{
    if [ ! -r "${PIDFILE}" ]; then
        echo "${PROG} is not running"
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
    restart)
        stop && sleep 1 && start
        ;;
    *)
        echo "Usage: $0 {start|stop|restart}"
        ;;
esac
