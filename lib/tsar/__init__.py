"""\
tsar provides a simple HTTP API for archiving and retrieving time-series
data. It uses the `Redis`_ key-value store to provide fast access to
stored records. New records can be sent via HTTP POST singly or in bulk.
Clients may request ranges of data limited by timestamps; the server can
perform a number of common tasks (ie downsampling) on the resulting data
before returning it to the client.

.. _Redis:      http://www.redis-db.com/
"""

__project__ = "tsar"
__version__ = "0.1"
__pkg__ = "tsar"
__description__ = "time series archival and retrieval"
__author__ = "Will Maier"
__author_email__ = "wcmaier@hep.wisc.edu"
__url__ = "http://code.hep.wisc.edu/tsar"

# See http://pypi.python.org/pypi?%3Aaction=list_classifiers.
__classifiers__ = [
    "Development Status :: 3 - Alpha",
    "Intended Audience :: Science/Research",
    "Intended Audience :: System Administrators",
    "License :: OSI Approved :: BSD License",
    "Natural Language :: English",
    "Programming Language :: Python :: 2.5",
    "Programming Language :: Python :: 2.6",
    "Topic :: Database :: Front-Ends",
    "Topic :: Internet :: WWW/HTTP :: HTTP Servers",
    "Topic :: System :: Monitoring",
    ] 
__keywords__ = "time series monitor"

__requires__ = [
    "redis",
    "neat",
    "pyCLI",
]

# The following is modeled after the ISC license.
__copyright__ = """\
Copyright (c) 2010 Will Maier <wcmaier@hep.wisc.edu>

Permission to use, copy, modify, and distribute this software for any
purpose with or without fee is hereby granted, provided that the above
copyright notice and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
"""

__todo__ = """\
""".split(" * ")
