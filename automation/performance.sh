#!/bin/sh
. ./configDefaults.sh


PERFORMANCE_DIR=$FILES_DIRECTORY"/performance"

if [[ ! -e $PERFORMANCE_DIR ]]; then
	mkdir $PERFORMANCE_DIR;
	echo "message,word count,wc without combine, connected components,kmeans,TPCH3" >>$PERFORMANCE_DIR"/executiontime.csv"
fi

message=`date +%Y-%m-%d`
while getopts "m:" OPTION; do
	case $OPTION in
		m) message=$OPTARG ;;
		\?) exit 1;;
	esac
done

start=$(date +%s)
./runWC-JAPI.sh
end=$(date +%s)
secWC=$(($end - $start))

start=$(date +%s)
./runWC-JAPI-withoutCombine.sh
end=$(date +%s)
secWCWithoutCombine=$(($end - $start))

start=$(date +%s)
./runCP-JAPI.sh
end=$(date +%s)
secCP=$(($end - $start))

start=$(date +%s)
./runKMeansMultiDimension-JAPI.sh
end=$(date +%s)
secKMeansMultiDimension=$(($end - $start))

start=$(date +%s)
./runTPCH3-JAPI.sh
end=$(date +%s)
secTPCH3=$(($end - $start))




echo $message","$secWC","$secWCWithoutCombine","$secCP","$secKMeansMultiDimension","$secTPCH3 >> $PERFORMANCE_DIR"/executiontime.csv"

python plot.py $PERFORMANCE_DIR"/executiontime.csv"