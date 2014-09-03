package com.github.projectflink.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public class WordCountGrouping {
	public static void main(String[] args) {
		String master = args[0];
		String inFile = args[1];
		String outFile = args[2];
		int numParts = Integer.valueOf(args[3]);
		System.out.println("Starting spark with master="+master+" in="+inFile+" out="+outFile);
		
		SparkConf conf = new SparkConf().setAppName("WordCountGrouping").setMaster(master);
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> file = sc.textFile(inFile);
		JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {
					public Iterable<String> call(String s) {
						return Arrays.asList(s.split(" "));
					}
				});
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});
		
		JavaPairRDD<String, Integer> counts  = pairs.groupByKey(numParts).mapToPair(new PairFunction<Tuple2<String,Iterable<Integer>>, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Iterable<Integer>> group) throws Exception {
				int count = 0;
				for(Integer a : group._2  ) {
					count += a;
				}
				return new Tuple2<String, Integer>(group._1, count);
			}
		});
		counts.saveAsTextFile(outFile);
	}
}
