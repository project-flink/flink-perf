package com.github.projectflink.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {
	public static void main(String[] args) {
		String master = args[0];
		String inFile = args[1];
		String outFile = args[2];
		System.err.println("Starting spark with master="+master+" in="+inFile+" out="+outFile);
		
		SparkConf conf = new SparkConf().setAppName("WordCount").setMaster(master);
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> file = sc.textFile(inFile);
		JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {
					public Iterable<String> call(String s) {
						return Arrays.asList( s.toLowerCase().split("\\W+") );
					}
				});
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		counts.saveAsTextFile(outFile);
	}
}
