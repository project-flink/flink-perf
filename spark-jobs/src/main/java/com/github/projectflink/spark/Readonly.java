package com.github.projectflink.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class Readonly {
	public static void main(String[] args) {
		String master = args[0];
		String inFile = args[1];
		System.err.println("Starting spark with master="+master+" in="+inFile);
		
		SparkConf conf = new SparkConf().setAppName("Read only job").setMaster(master).set("spark.hadoop.validateOutputSpecs", "false");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> file = sc.textFile(inFile);
		JavaRDD<String> res = file.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String arg0) throws Exception {
				return false;
			}
		});
		res.saveAsTextFile("file:///tmp/out");
	}
}
