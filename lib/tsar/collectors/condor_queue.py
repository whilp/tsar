#!/usr/bin/env python

"""condor_q -global -xml -attributes"""

from . import Collector

@Collector(name="condor-queue")
def condor_queue(app):
    pass

if __name__ == "__main__":
    condor_queue.run()
