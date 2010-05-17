#!/bin/sh

OMIT="tests,docs,/home/will/share/venv,/home/will/share/neat,/home/will/share/redis-py,setup"

coverage run --branch setup.py test
coverage report -m --omit $OMIT >| coverage.txt
cat coverage.txt
