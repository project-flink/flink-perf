#!/bin/bash

LINES=$1
if [[ -z "$LINES" ]]; then
	LINES=500
	echo "No argument passed. number of lines set to 500. Pass the number of lines as the first argument"
fi
echo "LINES=$LINES"

echo "Generating Data that is required for running the tasks"

. ./config.sh

cd $FILES_DIRECTORY
mkdir -p wc-data
cd wc-data

if [[ ! -f "en-common.wl.utf8" ]]; then
	echo "downloading dictionary"
	wget ftp://ftp.gnu.org/gnu/aspell/dict/en/aspell6-en-7.1-0.tar.bz2

	tar jxf aspell6-en-7.1-0.tar.bz2
	find aspell6-en-7.1-0 -name '*.cwl' -exec cp '{}' './' \;
	preunzip *.cwl
	for wl in *.wl; do
	    iconv --from-code=ISO-8859-1 --to-code=UTF-8 $wl | \
	    cut -d '/' -f 1 > $wl.utf8
	done
	rm *.wl
fi

ruby -e 'a=STDIN.readlines;'$LINES'.times do;b=[];15.times do; b << a[rand(a.size)].chomp end; puts b.join(" "); end' < en-common.wl.utf8 > generated-wc.txt

echo "done. find the generated file in "`pwd`"/generated-wc.txt"


