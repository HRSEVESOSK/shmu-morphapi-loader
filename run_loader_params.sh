#!/bin/sh
APPLICATION_PATH=/usr/local/src/shmu-morphapi-loader/
cd "${APPLICATION_PATH}"
python3 loader.py -begin $1 -end $2 > logs/loader_$1_$2_$(date +%Y-%m-%dT%H-%M-%S).log 2>&1
