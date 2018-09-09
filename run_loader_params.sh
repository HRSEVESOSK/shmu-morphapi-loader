#!/bin/sh
APPLICATION_PATH=/usr/local/src/shmu-morphapi-loader/
cd "${APPLICATION_PATH}"
python loader.py -begin $1 -end $2 > logs/$(date +%Y-%m-%dT%H-%M-%S).log 2>&1