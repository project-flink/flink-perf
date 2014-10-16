package com.github.projectflink.spark;

import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Grep {

	public static void main(String[] args) {
		String master = args[0];
		String inFile = args[1];
		String outFile = args[2];
		final String regex = args[3];
		System.err.println("Starting spark with master="+master+" in="+inFile);
		
		SparkConf conf = new SparkConf().setAppName("Read only job").setMaster(master).set("spark.hadoop.validateOutputSpecs", "false");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> file = sc.textFile(inFile);
		JavaRDD<String> res = file.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;
			Pattern p = Pattern.compile(regex);

			@Override
			public Boolean call(String value) throws Exception {
				if(value == null || value.length() == 0) {
					return false;
				}
				final Matcher m = p.matcher(value);
				if(m.find() ) {
					return true;
				}
				return false;
			}
		});
		res.saveAsTextFile(outFile);

	}
}
