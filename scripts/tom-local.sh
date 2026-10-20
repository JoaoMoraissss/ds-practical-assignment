#!/bin/bash
# Quick demo of the totally-ordered multicast on localhost (4 peers).

set -e
cd "$(dirname "$0")/.."

./scripts/build.sh

trap 'kill $(jobs -p) 2>/dev/null' EXIT

java -cp build ds.assignment.tom.Peer :7001 :7002 :7003 --port=7000 --name=peer0 --rate=6 &
sleep 0.2
java -cp build ds.assignment.tom.Peer :7000 :7002 :7003 --port=7001 --name=peer1 --rate=6 &
sleep 0.2
java -cp build ds.assignment.tom.Peer :7000 :7001 :7003 --port=7002 --name=peer2 --rate=6 &
sleep 0.2
java -cp build ds.assignment.tom.Peer :7000 :7001 :7002 --port=7003 --name=peer3 --rate=6 &

wait
