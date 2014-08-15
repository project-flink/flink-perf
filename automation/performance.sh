#!/bin/sh
. ./configDefaults.sh


PERFORMANCE_DIR=$FILES_DIRECTORY"/performance"

if [[ ! -e $PERFORMANCE_DIR ]]; then
	mkdir $PERFORMANCE_DIR;
	echo "message,word count,connected components,kmeans,testjob" >>$PERFORMANCE_DIR"/executiontime.csv"
fi

message=`date +%Y-%m-%d`
while getopts "m:" OPTION; do
	case $OPTION in
		m) message=$OPTARG ;;
		\?) exit 1;;
	esac
done

start=$(date +%s)
./runWC.sh
end=$(date +%s)
secWC=$(($end - $start))

start=$(date +%s)
./runCP.sh
end=$(date +%s)
secCP=$(($end - $start))

start=$(date +%s)
./runKMeansMultiDimension.sh
end=$(date +%s)
secKMeansMultiDimension=$(($end - $start))

start=$(date +%s)
./runTestjob.sh
end=$(date +%s)
secTestjob=$(($end - $start))


echo "$message,"$secWC","$secCP","$secKMeansMultiDimension","$secTestjob >> $PERFORMANCE_DIR"/executiontime.csv"

python plot.py $PERFORMANCE_DIR"/executiontime.csv"