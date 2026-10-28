#!/bin/bash
# Checks that every peer of the last tom-local.sh run delivered the same words
# in the same order (comparing up to the length of the shortest log).
cd "$(dirname "$0")/.."
n=$(wc -l logs/tom-p*.txt | grep -v total | awk '{print $1}' | sort -n | head -1)
echo "comparing the first $n delivered messages of each peer"
ok=1
for f in logs/tom-p*.txt; do
    if head -n "$n" "$f" | cmp -s - <(head -n "$n" logs/tom-p1.txt); then
        echo "  $f: same order as p1"
    else
        echo "  $f: DIFFERENT"; ok=0
    fi
done
[ $ok -eq 1 ] && echo "total order OK" || { echo "total order BROKEN"; exit 1; }
