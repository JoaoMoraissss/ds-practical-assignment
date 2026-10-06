#!/bin/bash
# Compiles all sources into build/.
set -e
cd "$(dirname "$0")/.."

rm -rf build
mkdir -p build
find src -name '*.java' -print0 | xargs -0 javac -d build
echo "compiled into build/"
