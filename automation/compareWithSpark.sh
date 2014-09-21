#!/bin/sh
. ./configDefaults.sh


PERFORMANCE_DIR=$FILES_DIRECTORY"/performance"

if [[ ! -e $PERFORMANCE_DIR ]]; then
	mkdir $PERFORMANCE_DIR;
fi

echo "system,word count,wc without combine, connected components,kmeans(low dimension),kmeans(high dimension),TPCH3,Page Rank" > $PERFORMANCE_DIR"/compExecutiontime.csv"

start=$(date +%s)
./runSpark-WC-Java.sh
end=$(date +%s)
secWC_spark=$(($end - $start))

start=$(date +%s)
./runSpark-WCgrouping-Java.sh
end=$(date +%s)
secWCWithoutCombine_spark=$(($end - $start))

start=$(date +%s)
./runSpark-CP-Java.sh
end=$(date +%s)
secCP_spark=$(($end - $start))

start=$(date +%s)
./runSpark-KMeansLowD-Java.sh
end=$(date +%s)
secKMeansLowDimension_spark=$(($end - $start))

start=$(date +%s)
./runSpark-KMeansHighD-Java.sh
end=$(date +%s)
secKMeansHighDimension_spark=$(($end - $start))

start=$(date +%s)
./runSpark-TPCH3-Java.sh
end=$(date +%s)
secTPCH3_spark=$(($end - $start))

start=$(date +%s)
./runSpark-PageRank-Java.sh
end=$(date +%s)
secPageRank_spark=$(($end - $start))

echo "spark,"$secWC_spark","$secWCWithoutCombine_spark","$secCP_spark","$secKMeansLowDimension_spark","$secKMeansHighDimension_spark","$secTPCH3_spark","$secPageRank_spark >> $PERFORMANCE_DIR"/compExecutiontime.csv"


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

echo "flink,"$secWC","$secWCWithoutCombine","$secCP","$secKMeansLowDimension","$secKMeansHighDimension","$secTPCH3","$secPageRank >> $PERFORMANCE_DIR"/compExecutiontime.csv"

python plotComparison.py $PERFORMANCE_DIR"/compExecutiontime.csv"