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

perf stat build/release/1blc measurements.txt > build/release/measurements_result.txt 2> build/release/statistics.txt
if cmp --silent -- measurements-result-expected.txt build/release/measurements_result.txt ; then
	date >> statistics.txt
	cat build/release/statistics.txt >> statistics.txt
	echo "OK"
else
	echo "FAIL"
	exit 1
fi
