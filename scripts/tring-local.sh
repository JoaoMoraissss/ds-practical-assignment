#!/bin/bash
# Exercise 1 on a single machine: the server and the 5 peers of the ring run as
# local processes on different ports. Ctrl-C stops everything.
set -e
cd "$(dirname "$0")/.."

./scripts/build.sh

SERVER_PORT=6000
BASE_PORT=5000   # peer pN listens on BASE_PORT + N

trap 'kill $(jobs -p) 2>/dev/null' EXIT

java -cp build ds.assignment.tring.Server $SERVER_PORT &
sleep 1

for i in 1 2 3 4 5; do
    next=$(( i % 5 + 1 ))
    token=""
    if [ "$i" -eq 1 ]; then token="--token"; fi
    java -cp build ds.assignment.tring.Peer \
        localhost:$(( BASE_PORT + next )) localhost:$SERVER_PORT \
        --port $(( BASE_PORT + i )) --name p$i $token &
done

wait
