#!/bin/bash
# Exercise 3 on a single machine: 6 peers run as local processes on different
# ports, each one knowing all the others. Ctrl-C stops everything.
#
#   ./scripts/tom-local.sh          6 honest peers, 1 word per second each
#   ./scripts/tom-local.sh past     p6 reuses old timestamps ("rewriting history")
#   ./scripts/tom-local.sh future   p6 uses timestamps far ahead ("writing in the future")
#
# Every peer also writes the words it delivers to logs/tom-pN.txt; if the order
# is total, these files are equal (compare them with: ./scripts/tom-check.sh).
set -e
cd "$(dirname "$0")/.."

./scripts/build.sh

BASE_PORT=8000   # peer pN listens on BASE_PORT + N
mkdir -p logs
rm -f logs/tom-p*.txt

trap 'kill $(jobs -p) 2>/dev/null' EXIT

for i in 1 2 3 4 5 6; do
    others=""
    for j in 1 2 3 4 5 6; do
        [ "$i" -ne "$j" ] && others="$others localhost:$(( BASE_PORT + j ))"
    done
    evil=""
    if [ "$i" -eq 6 ] && [ -n "$1" ]; then evil="--evil $1"; fi
    java -cp build ds.assignment.tom.Peer $others \
        --port $(( BASE_PORT + i )) --name p$i $evil \
        | tee >(grep --line-buffered ' deliver ' | sed -u 's/.*deliver //' > logs/tom-p$i.txt) &
done

wait
