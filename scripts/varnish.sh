# Configuration file for varnish
#
# /etc/init.d/varnish expects the variable $DAEMON_OPTS to be set from this
# shell script fragment.
#

# Maximum number of open files (for ulimit -n)
NFILES=131072

# Locked shared memory (for ulimit -l)
# Default log size is 82MB + header
MEMLOCK=82000

# Maximum size of corefile (for ulimit -c). Default in Fedora is 0
# DAEMON_COREFILE_LIMIT="unlimited"

# This file contains 4 alternatives, please use only one.

## Alternative 1, Minimal configuration, no VCL
#
# Listen on port 6081, administration on localhost:6082, and forward to
# content server on localhost:8080.  Use a fixed-size cache file.
#
#DAEMON_OPTS="-a :6081 \
#             -T localhost:6082 \
#             -b localhost:8080 \
#             -u varnish -g varnish \
#             -s file,/var/lib/varnish/varnish_storage.bin,1G"


## Alternative 2, Configuration with VCL
#
# Listen on port 6081, administration on localhost:6082, and forward to
# one content server selected by the vcl file, based on the request.  Use a
# fixed-size cache file.
#
DAEMON_OPTS="-a :80 \
             -f /scratch/tsar/scripts/tsar.vcl \
             -u apache -g apache \
			 -p thread_pool_min=200 \
			 -p thread_pool_max=4096 \
			 -p thread_pools=8 \
			 -p thread_pool_add_delay=2 \
			 -p listen_depth=4096 \
             -s file,/scratch/varnish/varnish_storage.bin,1G"
 

## Alternative 3, Advanced configuration
#
# See varnishd(1) for more information.
#
# # Main configuration file. You probably want to change it :)
# VARNISH_VCL_CONF=/etc/varnish/default.vcl
#
# # Default address and port to bind to
# # Blank address means all IPv4 and IPv6 interfaces, otherwise specify
# # a host name, an IPv4 dotted quad, or an IPv6 address in brackets.
# VARNISH_LISTEN_ADDRESS=
# VARNISH_LISTEN_PORT=6081
#
# # Telnet admin interface listen address and port
# VARNISH_ADMIN_LISTEN_ADDRESS=127.0.0.1
# VARNISH_ADMIN_LISTEN_PORT=6082
#
# # The minimum number of worker threads to start
# VARNISH_MIN_THREADS=1
#
# # The Maximum number of worker threads to start
# VARNISH_MAX_THREADS=1000
#
# # Idle timeout for worker threads
# VARNISH_THREAD_TIMEOUT=120
#
# # Cache file location
# VARNISH_STORAGE_FILE=/var/lib/varnish/varnish_storage.bin
#
# # Cache file size: in bytes, optionally using k / M / G / T suffix,
# # or in percentage of available disk space using the % suffix.
# VARNISH_STORAGE_SIZE=1G
#
# # Backend storage specification
# VARNISH_STORAGE="file,${VARNISH_STORAGE_FILE},${VARNISH_STORAGE_SIZE}"
#
# # Default TTL used when the backend does not specify one
# VARNISH_TTL=120
#
# # DAEMON_OPTS is used by the init script.  If you add or remove options, make
# # sure you update this section, too.
# DAEMON_OPTS="-a ${VARNISH_LISTEN_ADDRESS}:${VARNISH_LISTEN_PORT} \
#              -f ${VARNISH_VCL_CONF} \
#              -T ${VARNISH_ADMIN_LISTEN_ADDRESS}:${VARNISH_ADMIN_LISTEN_PORT} \
#              -t ${VARNISH_TTL} \
#              -w ${VARNISH_MIN_THREADS},${VARNISH_MAX_THREADS},${VARNISH_THREAD_TIMEOUT} \
#              -u varnish -g varnish \
#              -s ${VARNISH_STORAGE}"
#


## Alternative 4, Do It Yourself. See varnishd(1) for more information.
#
# DAEMON_OPTS=""
