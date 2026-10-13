#!/bin/bash
# Exercise 2 on a single machine: the 6 peers of the figure run as local
# processes on different ports. Ctrl-C stops everything.
#
#   ./scripts/p2p-local.sh          every peer starts with a random value
#   ./scripts/p2p-local.sh one      p1 starts with 1, the others with 0
set -e
cd "$(dirname "$0")/.."

./scripts/build.sh

BASE_PORT=7000   # peer pN listens on BASE_PORT + N

trap 'kill $(jobs -p) 2>/dev/null' EXIT

# Topology of the assignment: p1-p2, p2-p3, p2-p4, p4-p5, p4-p6.
# Each edge is listed once, on the peer that registers itself with the other.
declare -A CONTACTS=( [1]="2" [2]="4" [3]="2" [4]="" [5]="4" [6]="4" )

for i in 1 2 3 4 5 6; do
    contacts=""
    for n in ${CONTACTS[$i]}; do
        contacts="$contacts localhost:$(( BASE_PORT + n ))"
    done
    value=""
    if [ "$1" = "one" ]; then
        if [ "$i" -eq 1 ]; then value="--value 1"; else value="--value 0"; fi
    fi
    java -cp build ds.assignment.p2p.Peer $contacts \
        --port $(( BASE_PORT + i )) --name p$i $value &
done

wait
