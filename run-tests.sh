#!/bin/bash -x
memcached &

python setup.py testr --slowest --testr-args="$*"

ret=$?
kill $(jobs -p)
exit $ret
