#!/usr/bin/env sh
set -e

if [ ! -f measurements.txt ]; then
	echo "Please generate the measurements.txt file"
	exit 1
fi

if [ ! -f measurements-result-expected.txt ]; then
	echo "Please place the expected result here measurements-result-expected.txt file"
	exit 1
fi

if [ ! -f build/release/1blc ]; then
	echo "Please build the release binary first"
	exit 1
fi

if [ ! -f build/release/1blc ]; then
	echo "Please build the release binary first"
	exit 1
fi

if ! command -v hyperfine > /dev/null 2>&1 ; then
	echo "Hyperfine is not installed"
	exit 1
fi

hyperfine -w 3 'build/release/1blc measurements.txt > /dev/null'
