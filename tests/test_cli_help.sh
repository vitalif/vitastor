#!/bin/bash -ex

# Test that vitastor-cli --help knows every command name and alias the CLI accepts

. `dirname $0`/common.sh

for cmd in $(grep -o 'cmd\[0\] == "[^"]*"' src/cmd/cli.cpp | sed 's/.*== "//; s/"$//'); do
    build/src/cmd/vitastor-cli --help $cmd > ./testdata/help.txt 2>&1 || true
    if grep -q '^Unknown command' ./testdata/help.txt; then
        format_error "vitastor-cli --help $cmd does not know the command"
    fi
done

format_green OK
