#!/bin/bash
redis-server --daemonize yes
echo "-----------Redis Server Started-----------"
../../build/bin/ceph_test_rgw_directory
printf "\n-----------Directory Test Executed-----------\n"
redis-cli FLUSHALL
echo "-----------Redis Server Flushed-----------"
REDIS_PID=$(lsof -i4TCP:6379 -sTCP:LISTEN -t)
kill $REDIS_PID
echo "-----------Redis Server Stopped-----------"
