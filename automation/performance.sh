#!/bin/sh
. ./configDefaults.sh


PERFORMANCE_DIR=$FILES_DIRECTORY"/performance"

if [[ ! -e $PERFORMANCE_DIR ]]; then
	mkdir $PERFORMANCE_DIR;
	echo "message,word count,wc without combine, connected components,kmeans(low dimension),kmeans(high dimension),TPCH3,Page Rank" >>$PERFORMANCE_DIR"/executiontime.csv"
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
./runKMeansLowDimension-JAPI.sh
end=$(date +%s)
secKMeansLowDimension=$(($end - $start))

start=$(date +%s)
./runKMeansHighDimension-JAPI.sh
end=$(date +%s)
secKMeansHighDimension=$(($end - $start))

start=$(date +%s)
./runTPCH3-JAPI.sh
end=$(date +%s)
secTPCH3=$(($end - $start))

start=$(date +%s)
./runPageRank-JAPI.sh
end=$(date +%s)
secPageRank=$(($end - $start))



echo $message","$secWC","$secWCWithoutCombine","$secCP","$secKMeansLowDimension","$secKMeansHighDimension","$secTPCH3","$secPageRank >> $PERFORMANCE_DIR"/executiontime.csv"

python plot.py $PERFORMANCE_DIR"/executiontime.csv"